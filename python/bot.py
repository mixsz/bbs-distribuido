import os
import time
import zmq
import mensagem_pb2
from datetime import datetime

ENDERECO = os.environ.get("SERVIDOR_ENDERECO", "tcp://localhost:5555")
USUARIO = os.environ.get("USUARIO", "bot-python")
CANAL = os.environ.get("CANAL", "geral")

TIMEOUT_MS = 3000
TENTATIVAS = 10

context = zmq.Context()


def fmt(ts):
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def novo_socket():
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(ENDERECO)
    return s

socket = novo_socket()

def agora():
    return int(time.time() * 1000)

def enviar(tipo, payload):
    global socket
    for tentativa in range(1, TENTATIVAS + 1):
        socket.send_multipart([tipo, payload])
        try:
            return socket.recv_multipart()
        except zmq.Again:
            print(f"servidor sem resposta, tentativa {tentativa}/{TENTATIVAS}", flush=True)
            socket.close()
            socket = novo_socket()
    raise SystemExit("servidor nao respondeu, encerrando")

# 1. login
login_req = mensagem_pb2.LoginRequest()
login_req.usuario = USUARIO
login_req.timestamp = agora()
print(f"[ENVIADO] login | usuario={login_req.usuario} | timestamp={fmt(login_req.timestamp)}", flush=True)
time.sleep(1)

tipo, dados = enviar(b"login", login_req.SerializeToString())
login_resp = mensagem_pb2.LoginResponse()
login_resp.ParseFromString(dados)
print(f"[RECEBIDO] login | sucesso={login_resp.sucesso} | erro='{login_resp.erro}' | timestamp={fmt(login_resp.timestamp)}", flush=True)
time.sleep(1)

# 2. criar canal
canal_req = mensagem_pb2.CriarCanalRequest()
canal_req.nome_canal = CANAL
canal_req.timestamp = agora()
print(f"[ENVIADO] criar_canal | nome_canal={canal_req.nome_canal} | timestamp={fmt(canal_req.timestamp)}", flush=True)
time.sleep(1)

tipo, dados = enviar(b"criar_canal", canal_req.SerializeToString())
canal_resp = mensagem_pb2.CriarCanalResponse()
canal_resp.ParseFromString(dados)
print(f"[RECEBIDO] criar_canal | sucesso={canal_resp.sucesso} | erro='{canal_resp.erro}' | timestamp={fmt(canal_resp.timestamp)}", flush=True)
time.sleep(1)

# 3. listar canais
listar_req = mensagem_pb2.ListarCanaisRequest()
listar_req.timestamp = agora()
print(f"[ENVIADO] listar_canais | timestamp={fmt(listar_req.timestamp)}", flush=True)
time.sleep(1)

tipo, dados = enviar(b"listar_canais", listar_req.SerializeToString())
listar_resp = mensagem_pb2.ListarCanaisResponse()
listar_resp.ParseFromString(dados)
print(f"[RECEBIDO] listar_canais | canais={list(listar_resp.canais)} | timestamp={fmt(listar_resp.timestamp)}", flush=True)
time.sleep(1)