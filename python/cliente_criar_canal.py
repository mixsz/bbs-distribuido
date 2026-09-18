import zmq
import mensagem_pb2
import time

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")

canal_req = mensagem_pb2.CriarCanalRequest()
canal_req.nome_canal = "geral"
canal_req.timestamp = int(time.time() * 1000)

socket.send_multipart([b"criar_canal", canal_req.SerializeToString()])

tipo, dados = socket.recv_multipart()

if tipo != b"criar_canal":
    print("tipo de mensagem inesperado:", tipo)
    exit(1)

resposta = mensagem_pb2.CriarCanalResponse()
resposta.ParseFromString(dados)

print("sucesso:", resposta.sucesso)
print("erro:", resposta.erro)
print("timestamp:", resposta.timestamp)