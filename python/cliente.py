import zmq
import mensagem_pb2
import time

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

login_req = mensagem_pb2.LoginRequest()
login_req.usuario = "dan"
login_req.timestamp = int(time.time() * 1000)

socket.send_multipart([b"login", login_req.SerializeToString()])

tipo, dados = socket.recv_multipart()

if(tipo != b"login"):
    print("tipo de mensagem inesperado:", tipo)
    exit(1)

resposta = mensagem_pb2.LoginResponse()
resposta.ParseFromString(dados)

print("sucesso:", resposta.sucesso)
print("erro:", resposta.erro)
print("timestamp:", resposta.timestamp)