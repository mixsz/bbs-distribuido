import json
import os
import time
from datetime import datetime

import zmq
import mensagem_pb2

ARQUIVO_DADOS = os.environ.get("DADOS_PATH", "dados.json")
BROKER = os.environ.get("BROKER_ENDERECO", "tcp://localhost:5556")
PROXY_PUB = os.environ.get("PROXY_PUB_ENDERECO", "tcp://localhost:5557")

def fmt(ts):
    return datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S.%f")[:-3]

def agora():
    return int(time.time() * 1000)

def carregar_dados():
    dados = {"logins": [], "canais": [], "publicacoes": []}
    if os.path.exists(ARQUIVO_DADOS):
        with open(ARQUIVO_DADOS, "r") as f:
            dados.update(json.load(f))
    return dados

def salvar_dados(dados):
    with open(ARQUIVO_DADOS, "w") as f:
        json.dump(dados, f, indent=2)

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(BROKER)

pub = context.socket(zmq.PUB)
pub.connect(PROXY_PUB)

print(f"servidor python conectado ao broker em {BROKER} e ao proxy em {PROXY_PUB}", flush=True)

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

    elif tipo == b"publicar":
        pub_req = mensagem_pb2.PublicarRequest()
        pub_req.ParseFromString(msg_dados)
        time.sleep(0.2)
        print(f"[RECEBIDO] publicar | usuario={pub_req.usuario} | canal={pub_req.canal} | mensagem='{pub_req.mensagem}' | timestamp={fmt(pub_req.timestamp)}", flush=True)

        resposta = mensagem_pb2.PublicarResponse()
        if pub_req.canal.strip() == "":
            resposta.sucesso = False
            resposta.erro = "canal vazio"
        elif pub_req.mensagem.strip() == "":
            resposta.sucesso = False
            resposta.erro = "mensagem vazia"
        else:
            publicacao = mensagem_pb2.Publicacao(
                canal=pub_req.canal,
                mensagem=pub_req.mensagem,
                usuario=pub_req.usuario,
                timestamp=agora(),
            )
            pub.send_multipart([pub_req.canal.encode(), publicacao.SerializeToString()])
            print(f"[PUBLICADO] canal={publicacao.canal} | mensagem='{publicacao.mensagem}' | usuario={publicacao.usuario} | timestamp={fmt(publicacao.timestamp)}", flush=True)

            dados["publicacoes"].append({
                "canal": publicacao.canal,
                "mensagem": publicacao.mensagem,
                "usuario": publicacao.usuario,
                "timestamp": publicacao.timestamp,
            })
            salvar_dados(dados)
            resposta.sucesso = True
            resposta.erro = ""
        resposta.timestamp = pub_req.timestamp

        print(f"[ENVIADO] publicar | sucesso={resposta.sucesso} | erro='{resposta.erro}' | timestamp={fmt(resposta.timestamp)}", flush=True)
        socket.send_multipart([b"publicar", resposta.SerializeToString()])