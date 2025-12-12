import grpc
import chat4all_pb2
import chat4all_pb2_grpc

channel = grpc.insecure_channel("localhost:50051")
stub = chat4all_pb2_grpc.ChatServiceStub(channel)

try:
    res = stub.GetToken(chat4all_pb2.TokenRequest(
        client_id="admin_client",
        client_secret="super_secret_key"
    ))
    print("gRPC direct OK:", res.access_token, res.expires_in)
except grpc.RpcError as e:
    print("gRPC direct ERROR:", e.code(), e.details())
