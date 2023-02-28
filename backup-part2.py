from concurrent import futures
import argparse
import grpc
import ops_pb2
import ops_pb2_grpc

# Completely the same as part 1.
# Run with: python backup-part2.py 9001
# Should work with any functional port. Assumed that host is localhost.

class PrimaryBackupServiceServicer(ops_pb2_grpc.PrimaryBackupServiceServicer):
    d = {}
    def __init__(self):
        self.d = {}
                
    def putRequestOperation(self, request, context):
        self.d[request.key] = request.value
        return ops_pb2.replyPutRequest(response = "successful")

    def getRequestOperation(self, request, context):
        val = self.d.get(request.key, "not found")
        return ops_pb2.replyGetRequest(response = val)

def serve(hostname, numThreads):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=numThreads))
    ops_pb2_grpc.add_PrimaryBackupServiceServicer_to_server(PrimaryBackupServiceServicer(), server)
    server.add_insecure_port(hostname)
    server.start()
    print("Backup server started, listening on " + hostname)
    server.wait_for_termination()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("port")
    args = parser.parse_args()
    # Instructions state to assume host is localhost.
    hostname = "localhost:" + args.port
    serve(hostname, 10)