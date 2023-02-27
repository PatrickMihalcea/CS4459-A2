from concurrent import futures
import argparse
import grpc
import ops_pb2
import ops_pb2_grpc


class PrimaryBackupServiceServicer(ops_pb2_grpc.PrimaryBackupServiceServicer):
    d = {}
    def __init__(self, d):
        self.d = {}
                
    def putRequestOperation(self, request, context):
        self.d[request.key] = request.value
        return ops_pb2.replyPutRequest(response = "successful")

    def getRequestOperation(self, request, context):  
        val = self.d.get(request.key, None)
        if (val == None):
            return ops_pb2.replyGetRequest(response = "not Found")
        else:
            return ops_pb2.replyGetRequest(response = val)


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