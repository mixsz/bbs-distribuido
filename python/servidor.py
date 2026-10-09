import json
import os
import time
from datetime import datetime

import zmq
import mensagem_pb2

ARQUIVO_DADOS = os.environ.get("DADOS_PATH", "dados.json")
BROKER = os.environ.get("BROKER_ENDERECO", "tcp://localhost:5556")


def fmt(ts):
    return datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S")


def carregar_dados():
    if os.path.exists(ARQUIVO_DADOS):
        with open(ARQUIVO_DADOS, "r") as f:
            return json.load(f)
    return {"logins": [], "canais": []}


def salvar_dados(dados):
    with open(ARQUIVO_DADOS, "w") as f:
        json.dump(dados, f, indent=2)


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(BROKER)

print(f"servidor python conectado ao broker em {BROKER}", flush=True)

dados = carregar_dados()

while True:
    tipo, msg_dados = socket.recv_multipart()

    if tipo == b"login":
        login_req = mensagem_pb2.LoginRequest()
        login_req.ParseFromString(msg_dados)
        time.sleep(0.2)
        print(f"[RECEBIDO] login | usuario={login_req.usuario} | timestamp={fmt(login_req.timestamp)}", flush=True)

        resposta = mensagem_pb2.LoginResponse()
        if login_req.usuario.strip() == "":
            resposta.sucesso = False
            resposta.erro = "nome usuario vazio"
        else:
            resposta.sucesso = True
            resposta.erro = ""
            dados["logins"].append({"usuario": login_req.usuario, "timestamp": login_req.timestamp})
            salvar_dados(dados)
        resposta.timestamp = login_req.timestamp

        print(f"[ENVIADO] login | sucesso={resposta.sucesso} | erro='{resposta.erro}' | timestamp={fmt(resposta.timestamp)}", flush=True)
        time.sleep(0.2)
        socket.send_multipart([b"login", resposta.SerializeToString()])

    elif tipo == b"criar_canal":
        canal_req = mensagem_pb2.CriarCanalRequest()
        canal_req.ParseFromString(msg_dados)
        time.sleep(0.2)
        print(f"[RECEBIDO] criar_canal | nome_canal={canal_req.nome_canal} | timestamp={fmt(canal_req.timestamp)}", flush=True)

        resposta = mensagem_pb2.CriarCanalResponse()
        if canal_req.nome_canal.strip() == "":
            resposta.sucesso = False
            resposta.erro = "nome de canal vazio"
        elif canal_req.nome_canal in dados["canais"]:
            resposta.sucesso = False
            resposta.erro = "canal ja existe"
        else:
            dados["canais"].append(canal_req.nome_canal)
            salvar_dados(dados)
            resposta.sucesso = True
            resposta.erro = ""
        resposta.timestamp = canal_req.timestamp

        print(f"[ENVIADO] criar_canal | sucesso={resposta.sucesso} | erro='{resposta.erro}' | timestamp={fmt(resposta.timestamp)}", flush=True)
        time.sleep(0.2)
        socket.send_multipart([b"criar_canal", resposta.SerializeToString()])

    elif tipo == b"listar_canais":
        listar_req = mensagem_pb2.ListarCanaisRequest()
        listar_req.ParseFromString(msg_dados)
        time.sleep(0.2)
        print(f"[RECEBIDO] listar_canais | timestamp={fmt(listar_req.timestamp)}", flush=True)

        resposta = mensagem_pb2.ListarCanaisResponse()
        resposta.canais.extend(dados["canais"])
        resposta.timestamp = listar_req.timestamp

        print(f"[ENVIADO] listar_canais | canais={list(resposta.canais)} | timestamp={fmt(resposta.timestamp)}", flush=True)
        time.sleep(0.2)
        socket.send_multipart([b"listar_canais", resposta.SerializeToString()])