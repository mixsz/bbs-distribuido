import os
import random
import time
from datetime import datetime

import zmq
import mensagem_pb2

ENDERECO = os.environ.get("SERVIDOR_ENDERECO", "tcp://localhost:5555")
PROXY_SUB = os.environ.get("PROXY_SUB_ENDERECO", "tcp://localhost:5558")
USUARIO = os.environ.get("USUARIO", "bot-python")

TIMEOUT_MS = 3000
TENTATIVAS = 10
MIN_CANAIS = 5
MIN_INSCRICOES = 3
MENSAGENS_POR_RODADA = 10

FRASES = [
    "dale",
    "blz",
    "testando canal",
    "nova mensagem",
    "uau wow",
    "sla nao sei",
    "ola mundo",
    "urso",
]

context = zmq.Context()

def novo_socket():
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(ENDERECO)
    return s

socket = novo_socket()
sub = context.socket(zmq.SUB)
sub.connect(PROXY_SUB)
inscritos = set()

def fmt(ts):
    return datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S.%f")[:-3]

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

def processar_publicacoes():
    while sub.poll(0):
        topico, dados = sub.recv_multipart()
        canal = topico.decode()
        if canal not in inscritos:
            continue
        publicacao = mensagem_pb2.Publicacao()
        publicacao.ParseFromString(dados)
        print(f"[PUBLICACAO] canal={publicacao.canal} | mensagem='{publicacao.mensagem}' | usuario={publicacao.usuario} | enviada={fmt(publicacao.timestamp)} | recebida={fmt(agora())}", flush=True)

def esperar(segundos):
    fim = time.time() + segundos
    while True:
        restante = fim - time.time()
        if restante <= 0:
            break
        if sub.poll(int(restante * 1000) + 1):
            processar_publicacoes()

def login():
    req = mensagem_pb2.LoginRequest()
    req.usuario = USUARIO
    req.timestamp = agora()
    print(f"[ENVIADO] login | usuario={req.usuario} | timestamp={fmt(req.timestamp)}", flush=True)
    _, dados = enviar(b"login", req.SerializeToString())
    resp = mensagem_pb2.LoginResponse()
    resp.ParseFromString(dados)
    print(f"[RECEBIDO] login | sucesso={resp.sucesso} | erro='{resp.erro}' | timestamp={fmt(resp.timestamp)}", flush=True)

def listar_canais():
    req = mensagem_pb2.ListarCanaisRequest()
    req.timestamp = agora()
    print(f"[ENVIADO] listar_canais | timestamp={fmt(req.timestamp)}", flush=True)
    _, dados = enviar(b"listar_canais", req.SerializeToString())
    resp = mensagem_pb2.ListarCanaisResponse()
    resp.ParseFromString(dados)
    print(f"[RECEBIDO] listar_canais | canais={list(resp.canais)} | timestamp={fmt(resp.timestamp)}", flush=True)
    return list(resp.canais)

def criar_canal(nome):
    req = mensagem_pb2.CriarCanalRequest()
    req.nome_canal = nome
    req.timestamp = agora()
    print(f"[ENVIADO] criar_canal | nome_canal={req.nome_canal} | timestamp={fmt(req.timestamp)}", flush=True)
    _, dados = enviar(b"criar_canal", req.SerializeToString())
    resp = mensagem_pb2.CriarCanalResponse()
    resp.ParseFromString(dados)
    print(f"[RECEBIDO] criar_canal | sucesso={resp.sucesso} | erro='{resp.erro}' | timestamp={fmt(resp.timestamp)}", flush=True)
    return resp.sucesso

def publicar(canal, mensagem):
    req = mensagem_pb2.PublicarRequest()
    req.usuario = USUARIO
    req.canal = canal
    req.mensagem = mensagem
    req.timestamp = agora()
    print(f"[ENVIADO] publicar | canal={req.canal} | mensagem='{req.mensagem}' | timestamp={fmt(req.timestamp)}", flush=True)
    _, dados = enviar(b"publicar", req.SerializeToString())
    resp = mensagem_pb2.PublicarResponse()
    resp.ParseFromString(dados)
    print(f"[RECEBIDO] publicar | sucesso={resp.sucesso} | erro='{resp.erro}' | timestamp={fmt(resp.timestamp)}", flush=True)

login()
esperar(1)

while True:
    canais = listar_canais()

    if len(canais) < MIN_CANAIS:
        nome = f"canal-{random.randint(1000, 9999)}"
        if criar_canal(nome):
            canais.append(nome)

    if len(inscritos) < MIN_INSCRICOES:
        disponiveis = [c for c in canais if c not in inscritos]
        if disponiveis:
            escolhido = random.choice(disponiveis)
            sub.setsockopt(zmq.SUBSCRIBE, escolhido.encode())
            inscritos.add(escolhido)
            print(f"[INSCRICAO] canal={escolhido} | total={len(inscritos)}", flush=True)

    if not canais:
        esperar(1)
        continue

    canal = random.choice(canais)
    for i in range(1, MENSAGENS_POR_RODADA + 1):
        publicar(canal, f"{random.choice(FRASES)} #{i}")
        esperar(1)