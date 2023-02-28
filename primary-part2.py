import concurrent.futures
from concurrent import futures
import argparse
import grpc
import ops_pb2
import ops_pb2_grpc

# Run with: python primary-part2.py 9001
# Should work with any functional port. Assumed that host is localhost.

class PrimaryBackupServiceServicer(ops_pb2_grpc.PrimaryBackupServiceServicer):
    def __init__(self):
        self.d = {}
        self.sequenceNum = 0
        # Clear log contents.
        self.log = open("log.txt", "w")

        # Make list of ports.
        self.ports = []
        with open("location.txt", "r") as loc_file:
            for line in loc_file:
                self.ports.append(line)

        # Process Operations.
        with open('operations.txt') as topo_file:
            for line in topo_file:
                line = line.split()
                if (line[0] == "put"):
                    self.localPut(line[1], line[2])
                else:
                    self.localGet(line[1])            
        print(self.d)
    
    # PUT OPERATIONS.
    def putRequestOperation(self, request, context):
        result = self.localPut(request.key, request.value)
        return ops_pb2.replyPutRequest(response = result)
    
    def localPut(self, keyIn, valueIn):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(self.sendPut, keyIn, valueIn, port) for port in self.ports]
        for i in concurrent.futures.as_completed(results):
            with open("log.txt", "a") as f:
                f.write(i.result()[1] + " put " + keyIn + " " + valueIn +"\n")
        self.d[keyIn] = valueIn  
        with open("log.txt", "a") as f:
            f.write("put " + keyIn + " " + valueIn +"\n")
        return "successful"

    def sendPut(self, keyIn, valueIn, port):
        # Increment sequenceNum to track operations.
        self.sequenceNum += 1
        hostname = "localhost:" + port
        # Send put request to backup.
        with grpc.insecure_channel(hostname) as channel:
            stub = ops_pb2_grpc.PrimaryBackupServiceStub(channel)
            val = stub.putRequestOperation(ops_pb2.putRequest(\
                key=keyIn, value=valueIn, sequenceNumber=self.sequenceNum))
        result = [val.response, hostname]
        return result


    # GET OPERATIONS.
    def getRequestOperation(self, request, context): 
        result = self.localGet(request.key)
        return ops_pb2.replyGetRequest(response = result)
        
    def localGet(self, keyIn):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(self.sendGet, keyIn, port) for port in self.ports]

        for i in concurrent.futures.as_completed(results):
            with open("log.txt", "a") as f:
                f.write(i.result()[1] + " get " + keyIn + " " + i.result()[0] + "\n")

        result = self.d.get(keyIn, "not found")
        with open("log.txt", "a") as f:
            f.write("get " + keyIn + " " + result + "\n")
        return result
            
    def sendGet(self, keyIn, port):
        hostname = "localhost:" + port
        # Send get request to backup.
        with grpc.insecure_channel(hostname) as channel:
            stub = ops_pb2_grpc.PrimaryBackupServiceStub(channel)
            val = stub.getRequestOperation(ops_pb2.getRequest(key=keyIn))
        result =[val.response, hostname]
        return result







def serve(hostname, numThreads):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=numThreads))
    ops_pb2_grpc.add_PrimaryBackupServiceServicer_to_server(PrimaryBackupServiceServicer(), server)
    server.add_insecure_port(hostname)
    server.start()
    print("Primary server started, listening on " + hostname)
    server.wait_for_termination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("port")
    args = parser.parse_args()
    # Instructions state to assume host is localhost.
    hostname = "localhost:" + args.port
    serve(hostname, 10)