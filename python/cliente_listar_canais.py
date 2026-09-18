import zmq
import mensagem_pb2
import time

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

listar_req = mensagem_pb2.ListarCanaisRequest()
listar_req.timestamp = int(time.time() * 1000)

socket.send_multipart([b"listar_canais", listar_req.SerializeToString()])

tipo, dados = socket.recv_multipart()

if tipo != b"listar_canais":
    print("tipo de mensagem inesperado:", tipo)
    exit(1)

resposta = mensagem_pb2.ListarCanaisResponse()
resposta.ParseFromString(dados)

print("canais:", resposta.canais)
print("timestamp:", resposta.timestamp)