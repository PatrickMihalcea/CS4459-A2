from concurrent import futures
import argparse
import grpc
import ops_pb2
import ops_pb2_grpc


class PrimaryBackupServiceServicer(ops_pb2_grpc.PrimaryBackupServiceServicer):
    def __init__(self, d):
        self.d = {}
        self.sequenceNum = 0
        with open('operations.txt') as topo_file:
            for line in topo_file:
                line.split()
                if (line[0] == "put"):
                    self.d[line[1]] = line[2]
                    self.sendPut(line[1], line[2])
                else:
                    val = self.d.get(line[1], None)
                    self.sendGet(line[1])
                    

    def putRequestOperation(self, request, context):
        returnMessage = self.sendPut(request.key, request.value)
        if (returnMessage == "successful"):
            self.d[request.key] = request.value
            return ops_pb2.replyPutRequest(response = "successful")
        return ops_pb2.replyPutRequest(response = "unsuccessful")
    
    def getRequestOperation(self, request, context): 
        returnMessage = self.sendGet(request.key)
        if (returnMessage == None):
            return ops_pb2.replyGetRequest(response = "not Found")
        else:
            val = self.d.get(request.key, None)
            if (val == None):
                return ops_pb2.replyGetRequest(response = "not Found")
            else:
                return ops_pb2.replyGetRequest(response = val)
        
    def sendPut(self, keyIn, valueIn):
        self.sequenceNum += 1
        with grpc.insecure_channel('localhost:9001') as channel:
            stub = ops_pb2_grpc.PrimaryBackupServiceStub(channel)
            val = stub.putRequestOperation(\
                key = keyIn, value = valueIn, sequenceNumber = self.sequenceNum)
        return val
            
    def sendGet(self, keyIn):
        with grpc.insecure_channel('localhost:9001') as channel:
            stub = ops_pb2_grpc.PrimaryBackupServiceStub(channel)
            val = stub.getRequestOperation(key = keyIn)
        return val

def serve(hostname, numThreads):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=numThreads))
    ops_pb2_grpc.add_PrimaryBackupServiceServicer_to_server(PrimaryBackupServiceServicer(), server)
    server.add_insecure_port(hostname)
    server.start()
    print("Server started, listening on " + hostname)
    server.wait_for_termination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port")
    args = parser.parse_args()
    hostname = args.host + ":" + args.port
    serve(hostname, 10)