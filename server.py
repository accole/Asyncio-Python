import asyncio
import argparse
import logging
import time
import json
import async_timeout
import aiohttp
import socket


#Google API Key
#API_KEY = your api key

#server names with assigned ports
servers = ["Hill", "Jaquez", "Smith", "Campbell", "Singleton"]
#SEASNET ports
server_names = {"Hill": 12095, "Jaquez": 12096, "Smith": 12097, "Campbell": 12098, "Singleton": 12099}
#Local ports
#server_names = {"Hill": 8000, "Jaquez": 8001, "Smith": 8002, "Campbell": 8003, "Singleton": 8004}
#bidirectional dependencies
server_routes = {
    "Hill": ["Jaquez", "Smith"],
    "Jaquez": ["Hill", "Singleton"],
    "Smith": ["Hill", "Campbell", "Singleton"],
    "Campbell": ["Smith", "Singleton"],
    "Singleton": ["Jaquez", "Smith", "Campbell"]
}
#keywords that could be used by client
commands = ["IAMAT", "AT", "WHATSAT"]

#client information storage structure
clients = {}
tasks = {}

class Server:
    def __init__(self, name, ip='127.0.0.1', port=8888, message_max_length=1e6):
        self.name = name
        self.ip = ip
        self.port = server_names[name]
        self.message_max_length = int(message_max_length)


    async def send_to_client(self, writer, msg):
        #if message is empty don't waste overhead
        if msg == None:
            return
        #else
        else:
            try:
                #use writer to send encoded message and then EOF
                writer.write(msg.encode())
                await writer.drain()
                writer.write_eof()
            except:
                pass
            return


    async def flood(self, client, resp):
        #log flood
        logging.info("FLOODING INFO FOR CLIENT " + client + "\n")

        #send a flood message to all connected servers
        for server in server_routes[self.name]:
            if server != self.name:
                portnum = server_names[server]
                try:
                    #send message
                    reader, writer = await asyncio.open_connection('127.0.0.1', portnum, loop=self.loop)
                    logging.info('CONNECTED TO ' + server)
                    logging.info('FLOODING ' + client + ' TO ' + server)
                    await self.send_to_client(writer, resp)
                    logging.info('AT MESSAGE SENT: ' + resp)
                    logging.info('CLOSED CONNECTION TO ' + server + '\n')
                except:
                    #error sending flood message
                    logging.info('ERROR FLOODING %s TO %s' % (client, server))
        return


    async def handle_IAMAT(self, writer, command, rtime):
        #format:
        # IAMAT [client] [coordinates] [time_sent]
        split_comm = command
        client = split_comm[1]
        coord = split_comm[2]
        time_sent = float(split_comm[3])
        #find time difference
        t_diff = rtime - time_sent
        #find latitude and longitude from coordinates
        latitude, longitude = extract_coord(coord)

        #create client information for flooding
        clients[client] = {
            'server': self.name,
            'latitude': latitude,
            'longitude': longitude, 
            't_diff': t_diff,
            'time_sent': time_sent
        }

        #respond to client
        resp = "AT %s %s %s %s %s" % (self.name, str(t_diff), client, coord, time_sent)
        logging.info('IAMAT RESPONSE: ' + resp)
        await self.send_to_client(writer, resp)

        #flood the client info
        await self.flood(client, resp)
        return


    async def handle_WHATSAT(self, writer, command, rtime):
        #format:
        # WHATSAT [client] [radius] [upper_bound]
        split_comm = command
        client = split_comm[1]
        radius = float(split_comm[2])
        upper_bound = int(split_comm[3])
        t_diff = clients[client]['t_diff']
        server = clients[client]['server']

        #check if client is in client list before trying to access info
        if client not in clients:
            #log invalid message
            logging.info('CLIENT NOT IN DEVICE LIST: ' + client + '\n')
            resp = '? ' + command
            logging.info('WHATSAT RESPONSE: ' + resp)
            #send invalid response to client
            await self.send_to_client(writer, resp)
            return
        
        #get client coordinates from data structure
        latitude = clients[client]['latitude']
        longitude = clients[client]['longitude']
        loc = "{0},{1}".format(latitude, longitude)

        #query the google api
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={0}&location={1}&radius={2}".format(API_KEY, loc, radius)

        #from TA Github
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            #log query
            logging.info('QUERYING GOOGLE MAPS API FOR LOCATION ({0}, {1}) WITH RADIUS {2}'.format(latitude, longitude, radius))
            #query the google api and get json
            async with async_timeout.timeout(10):
                async with session.get(url) as response:
                    response = await response.json()
            
            #send a server response using query results
            #get the json format message
            if len(response['results']) > upper_bound:
                response = response['results'][:int(upper_bound)]
            js = json.dumps(response, indent=3)
            time_sent = clients[client]['time_sent']
            resp = "AT {} {} {} {} {}\n{}\n".format(server, str(t_diff), client, latitude + longitude, time_sent, js)
            #send responses
            logging.info('WHATSAT RESPONSE: ' + resp)
            await self.send_to_client(writer, resp)
            return



    async def handle_AT(self, writer, command):
        #format:
        # AT [server] [time_diff] [client] [coordinates] [time_sent]
        split_comm = command

        serv = split_comm[1]
        t_diff = float(split_comm[2])
        client = split_comm[3]
        coord = split_comm[4]
        time_sent = float(split_comm[5])

        #logging server information
        command = ''
        i = 0
        for s in split_comm:
            if i == len(split_comm) - 1:
                command = command + s
            else:
                command = command + s + " " 
            i = i + 1

        logging.info("RECEIVED FROM SERVER: " + command)

        #get latitude, longitude from coordinates
        latitude, longitude = extract_coord(coord)

        #test if duplicate command (already flooded on sending end)
        if client in clients:
            stored_time = float(clients[client]['time_sent'])
            if stored_time <= time_sent:
                logging.info('DUPLICATE FOR ' + client)
                return

        #update client information for flooding
        clients[client] = {
            'server': serv,
            'latitude': latitude,
            'longitude': longitude, 
            't_diff': t_diff,
            'time_sent': time_sent
        }
        
        #flood client info
        await self.flood(client, command)
        return



    async def handle_message(self, reader, writer):
        while not reader.at_eof():
            command = await reader.readline()
            rtime = time.time()
            split_comm = command.decode().split(' ')
            command = command.decode()

            #see if message command is in the command list
            if split_comm[0] not in commands:
                #invalid message - need valid command
                logging.info('INVALID COMMAND: ' + '? ' + command + '\n')
                await self.send_to_client(writer, "? " + command)

            #check for server-server communication
            elif split_comm[0] == commands[1] and len(split_comm) != 6:
                logging.info('INVALID COMMAND: ' + '? ' + command + '\n')
                await self.send_to_client(writer, "? " + command)

            #check command has right # of elements
            elif split_comm[0] != commands[1] and len(split_comm) != 4:
                #invalid message - need 4 arguments
                logging.info('INVALID COMMAND: ' + '? ' + command + '\n')
                await self.send_to_client(writer, "? " + command)

            #correct format of arguments
            else:
                c = split_comm[0]
                if c == commands[0]:
                    #IAMAT
                    logging.info('COMMAND RECEIVED: ' + command)
                    await self.handle_IAMAT(writer, split_comm, rtime)
                elif c == commands[1]:
                    #AT
                    await self.handle_AT(writer, split_comm)
                elif c == commands[2]:
                    #WHATSAT 
                    logging.info('COMMAND RECEIVED: ' + command)
                    await self.handle_WHATSAT(writer, split_comm, rtime)
        return


    async def handle_client(self, reader, writer):
        #handles client messages
        #creates an asynchronous task to handle so multiple clients can
        #talk to the server at once
        t = asyncio.create_task(self.handle_message(reader, writer))
        tasks[t] = (reader,writer)

        def close_connection(t):
            logging.info("CLOSING CONNECTION ...\n\n")
            del tasks[t]
            writer.close()

        #when done, close the writer and connection
        t.add_done_callback(close_connection)
    

    def run(self):
        #set up asyncio
        self.loop = asyncio.get_event_loop()
        #send all IO to member function read_message()
        coroutine = asyncio.start_server(self.handle_client, self.ip, self.port, loop=self.loop)
        serv = self.loop.run_until_complete(coroutine)

        #log Server information
        logging.info('Serving on {}'.format(serv.sockets[0].getsockname()) + '\n')

        #run until Keyboard interrupted
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        #close the server
        serv.close()
        self.loop.run_until_complete(serv.wait_closed())
        self.loop.close()
        return


def extract_coord(coordinate):
    #returns latitude, longitude
    second = False
    index = 0
    for char in coordinate:
        if char == '+' or char == '-':
            if second:
                #at the start of longitude
                lat = coordinate[:index]
                lon = coordinate[index:]
            else:
                second = True

        index = index + 1
    
    return lat, lon


#START HERE
def main():
    #argparse for reading in command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('server_name', type=str, help='required server name input')
    args = parser.parse_args()

    #error handling server_name
    s = args.server_name
    if s not in servers:
        print("Invalid server name.")
        exit(1)

    #configure logging file
    f = "./logs/server_{}.log".format(s)
    open(f, "w").close()
    logging.basicConfig(filename=f, filemode='w', level=logging.INFO)

    #begin the async server loop
    #create Server instance
    server = Server(s)
    server.run()
    return


if __name__ == '__main__':
    main()
