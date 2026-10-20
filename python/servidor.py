import json
import os
import time
from datetime import datetime

import zmq
import mensagem_pb2

ARQUIVO_DADOS = os.environ.get("DADOS_PATH", "dados.json")
BROKER = os.environ.get("BROKER_ENDERECO", "tcp://localhost:5556")
PROXY_PUB = os.environ.get("PROXY_PUB_ENDERECO", "tcp://localhost:5557")
REFERENCIA = os.environ.get("REFERENCIA_ENDERECO", "tcp://localhost:5559")
NOME = os.environ.get("NOME_SERVIDOR", "servidor-python")
PAUSA = int(os.environ.get("PAUSA_MS", "200")) / 1000

INTERVALO_HEARTBEAT = 10
TIMEOUT_MS = 3000
TENTATIVAS = 20

relogio = 0
desvio = int(os.environ.get("DESVIO_RELOGIO_MS", "0"))
mensagens_clientes = 0

def agora_sistema():
    return int(time.time() * 1000)

def agora():
    # relogio fisico do server: relogio do sistema + desvio
    return agora_sistema() + desvio

def fmt(ts):
    return datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S.%f")[:-3]


def pausa():
    if PAUSA > 0:
        time.sleep(PAUSA)


def proximo_relogio():
    global relogio
    relogio += 1
    return relogio


def atualizar_relogio(recebido):
    global relogio
    relogio = max(relogio, recebido)

def carregar_dados():
    dados = {"logins": [], "canais": [], "publicacoes": []}
    if os.path.exists(ARQUIVO_DADOS):
        with open(ARQUIVO_DADOS, "r") as f:
            dados.update(json.load(f))
    return dados

def salvar_dados(dados):
    with open(ARQUIVO_DADOS, "w") as f:
        f.write(json.dumps(dados, indent=2))

context = zmq.Context()

def novo_socket_referencia():
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(REFERENCIA)
    return s

ref = novo_socket_referencia()

def pedir_referencia(tipo, payload):
    global ref
    for tentativa in range(1, TENTATIVAS + 1):
        ref.send_multipart([tipo, payload])
        try:
            return ref.recv_multipart()
        except zmq.Again:
            print(f"referencia sem resposta, tentativa {tentativa}/{TENTATIVAS}", flush=True)
            ref.close()
            ref = novo_socket_referencia()
    raise SystemExit("referencia nao respondeu, encerrando")

def obter_rank():
    req = mensagem_pb2.RankRequest()
    req.nome = NOME
    req.timestamp = agora()
    req.relogio = proximo_relogio()
    print(f"[ENVIADO] rank | nome={req.nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)
    _, dados = pedir_referencia(b"rank", req.SerializeToString())
    resp = mensagem_pb2.RankResponse()
    resp.ParseFromString(dados)
    atualizar_relogio(resp.relogio)
    print(f"[RECEBIDO] rank | rank={resp.rank} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio} | local={relogio}", flush=True)
    return resp.rank

def listar_servidores():
    req = mensagem_pb2.ListaServidoresRequest()
    req.timestamp = agora()
    req.relogio = proximo_relogio()
    print(f"[ENVIADO] list | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)
    _, dados = pedir_referencia(b"list", req.SerializeToString())
    resp = mensagem_pb2.ListaServidoresResponse()
    resp.ParseFromString(dados)
    atualizar_relogio(resp.relogio)
    lista = [f"{s.nome}:{s.rank}" for s in resp.servidores]
    print(f"[RECEBIDO] list | servidores={lista} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio} | local={relogio}", flush=True)

def heartbeat():
    global desvio
    req = mensagem_pb2.HeartbeatRequest()
    req.nome = NOME
    req.timestamp = agora()
    req.relogio = proximo_relogio()
    print(f"[ENVIADO] heartbeat | nome={req.nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)

    t0 = agora_sistema()
    _, dados = pedir_referencia(b"heartbeat", req.SerializeToString())
    t1 = agora_sistema()

    resp = mensagem_pb2.HeartbeatResponse()
    resp.ParseFromString(dados)
    atualizar_relogio(resp.relogio)
    print(f"[RECEBIDO] heartbeat | ok={resp.ok} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio} | local={relogio}", flush=True)

    estimada = resp.timestamp + (t1 - t0) // 2  # hora CORRETA estimada = hora da referencia + metade do tempo de ida e volta
    novo_desvio = estimada - t1
    ajuste = novo_desvio - desvio
    desvio = novo_desvio
    print(f"[SINCRONIZACAO] referencia={fmt(resp.timestamp)} | rtt={t1 - t0}ms | ajuste={ajuste:+d}ms | desvio={desvio:+d}ms", flush=True)
    pausa()


rank = obter_rank()
print(f"servidor {NOME} cadastrado com rank {rank}", flush=True)
listar_servidores()

socket = context.socket(zmq.REP)
socket.connect(BROKER)

pub = context.socket(zmq.PUB)
pub.connect(PROXY_PUB)

print(f"servidor python conectado ao broker em {BROKER} e ao proxy em {PROXY_PUB}", flush=True)

dados = carregar_dados()

while True:
    tipo, msg_dados = socket.recv_multipart()
    mensagens_clientes += 1

    if tipo == b"login":
        login_req = mensagem_pb2.LoginRequest()
        login_req.ParseFromString(msg_dados)
        atualizar_relogio(login_req.relogio)
        pausa()
        print(f"[RECEBIDO] login | usuario={login_req.usuario} | timestamp={fmt(login_req.timestamp)} | relogio={login_req.relogio} | local={relogio}", flush=True)

        resposta = mensagem_pb2.LoginResponse()
        if login_req.usuario.strip() == "":
            resposta.sucesso = False
            resposta.erro = "nome usuario vazio"
        else:
            resposta.sucesso = True
            resposta.erro = ""
            dados["logins"].append({"usuario": login_req.usuario, "timestamp": login_req.timestamp})
            salvar_dados(dados)
        resposta.timestamp = agora()
        resposta.relogio = proximo_relogio()

        print(f"[ENVIADO] login | sucesso={resposta.sucesso} | erro='{resposta.erro}' | timestamp={fmt(resposta.timestamp)} | relogio={resposta.relogio}", flush=True)
        pausa()
        socket.send_multipart([b"login", resposta.SerializeToString()])

    elif tipo == b"criar_canal":
        canal_req = mensagem_pb2.CriarCanalRequest()
        canal_req.ParseFromString(msg_dados)
        atualizar_relogio(canal_req.relogio)
        pausa()
        print(f"[RECEBIDO] criar_canal | nome_canal={canal_req.nome_canal} | timestamp={fmt(canal_req.timestamp)} | relogio={canal_req.relogio} | local={relogio}", flush=True)

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
        resposta.timestamp = agora()
        resposta.relogio = proximo_relogio()

        print(f"[ENVIADO] criar_canal | sucesso={resposta.sucesso} | erro='{resposta.erro}' | timestamp={fmt(resposta.timestamp)} | relogio={resposta.relogio}", flush=True)
        pausa()
        socket.send_multipart([b"criar_canal", resposta.SerializeToString()])

    elif tipo == b"listar_canais":
        listar_req = mensagem_pb2.ListarCanaisRequest()
        listar_req.ParseFromString(msg_dados)
        atualizar_relogio(listar_req.relogio)
        pausa()
        print(f"[RECEBIDO] listar_canais | timestamp={fmt(listar_req.timestamp)} | relogio={listar_req.relogio} | local={relogio}", flush=True)

        resposta = mensagem_pb2.ListarCanaisResponse()
        resposta.canais.extend(dados["canais"])
        resposta.timestamp = agora()
        resposta.relogio = proximo_relogio()

        print(f"[ENVIADO] listar_canais | canais={list(resposta.canais)} | timestamp={fmt(resposta.timestamp)} | relogio={resposta.relogio}", flush=True)
        pausa()
        socket.send_multipart([b"listar_canais", resposta.SerializeToString()])

    elif tipo == b"publicar":
        pub_req = mensagem_pb2.PublicarRequest()
        pub_req.ParseFromString(msg_dados)
        atualizar_relogio(pub_req.relogio)
        pausa()
        print(f"[RECEBIDO] publicar | usuario={pub_req.usuario} | canal={pub_req.canal} | mensagem='{pub_req.mensagem}' | timestamp={fmt(pub_req.timestamp)} | relogio={pub_req.relogio} | local={relogio}", flush=True)

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
                relogio=proximo_relogio(),
            )
            pub.send_multipart([pub_req.canal.encode(), publicacao.SerializeToString()])
            print(f"[PUBLICADO] canal={publicacao.canal} | mensagem='{publicacao.mensagem}' | usuario={publicacao.usuario} | timestamp={fmt(publicacao.timestamp)} | relogio={publicacao.relogio}", flush=True)

            dados["publicacoes"].append({
                "canal": publicacao.canal,
                "mensagem": publicacao.mensagem,
                "usuario": publicacao.usuario,
                "timestamp": publicacao.timestamp,
            })
            salvar_dados(dados)
            resposta.sucesso = True
            resposta.erro = ""
        resposta.timestamp = agora()
        resposta.relogio = proximo_relogio()

        print(f"[ENVIADO] publicar | sucesso={resposta.sucesso} | erro='{resposta.erro}' | timestamp={fmt(resposta.timestamp)} | relogio={resposta.relogio}", flush=True)
        socket.send_multipart([b"publicar", resposta.SerializeToString()])

    else:
        print(f"tipo de mensagem desconhecido: {tipo}", flush=True)
        socket.send_multipart([tipo, b""])

    if mensagens_clientes % INTERVALO_HEARTBEAT == 0:
        heartbeat()
        listar_servidores()