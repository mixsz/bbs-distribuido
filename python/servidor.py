import zmq
import mensagem_pb2
import json
import os

ARQUIVO_DADOS = "dados.json"

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
socket.bind("tcp://*:5555")

print("servidor rodando na porta 5555...", flush=True)

dados = carregar_dados()

while True:
    tipo, msg_dados = socket.recv_multipart()

    if tipo == b"login":
        login_req = mensagem_pb2.LoginRequest()
        login_req.ParseFromString(msg_dados)
        print(f"login recebido: {login_req.usuario}", flush=True)

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

        socket.send_multipart([b"login", resposta.SerializeToString()])

    elif tipo == b"criar_canal":
        canal_req = mensagem_pb2.CriarCanalRequest()
        canal_req.ParseFromString(msg_dados)
        print(f"canal a ser criado: {canal_req.nome_canal}", flush=True)

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

        socket.send_multipart([b"criar_canal", resposta.SerializeToString()])

    elif tipo == b"listar_canais":
        listar_req = mensagem_pb2.ListarCanaisRequest()
        listar_req.ParseFromString(msg_dados)
        print("listar canais recebido", flush=True)

        resposta = mensagem_pb2.ListarCanaisResponse()
        resposta.canais.extend(dados["canais"])
        resposta.timestamp = listar_req.timestamp

        socket.send_multipart([b"listar_canais", resposta.SerializeToString()])