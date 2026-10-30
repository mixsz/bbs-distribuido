import os
import time
from datetime import datetime

import zmq
import mensagem_pb2

PORTA = 5559
TIMEOUT_SERVIDOR = float(os.environ.get("TIMEOUT_SERVIDOR", "30"))
PAUSA = int(os.environ.get("PAUSA_MS", "200")) / 1000

relogio = 0
proximo_rank = 1
ranks = {}
ativos = {}


def agora():
    return int(time.time() * 1000)


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


def registrar(nome):
    global proximo_rank
    if nome not in ranks:
        ranks[nome] = proximo_rank
        proximo_rank += 1
        print(f"[REGISTRADO] servidor={nome} | rank={ranks[nome]}", flush=True)
    elif nome not in ativos:
        print(f"[REATIVADO] servidor={nome} | rank={ranks[nome]}", flush=True)
    ativos[nome] = time.time()
    return ranks[nome]


def remover_inativos():
    limite = time.time() - TIMEOUT_SERVIDOR
    for nome in [n for n, visto in ativos.items() if visto < limite]:
        del ativos[nome]
        print(f"[REMOVIDO] servidor={nome} | rank={ranks[nome]} | motivo=sem heartbeat ha mais de {TIMEOUT_SERVIDOR:.0f}s", flush=True)


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind(f"tcp://*:{PORTA}")

print(f"referencia rodando na porta {PORTA} | timeout de heartbeat={TIMEOUT_SERVIDOR:.0f}s", flush=True)

while True:
    if not socket.poll(1000):
        remover_inativos()
        continue

    tipo, dados = socket.recv_multipart()
    remover_inativos()

    if tipo == b"rank":
        req = mensagem_pb2.RankRequest()
        req.ParseFromString(dados)
        atualizar_relogio(req.relogio)
        pausa()
        print(f"[RECEBIDO] rank | nome={req.nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)

        resposta = mensagem_pb2.RankResponse()
        resposta.rank = registrar(req.nome) if req.nome.strip() else 0
        resposta.timestamp = agora()
        resposta.relogio = proximo_relogio()

        print(f"[ENVIADO] rank | rank={resposta.rank} | timestamp={fmt(resposta.timestamp)} | relogio={resposta.relogio}", flush=True)
        pausa()
        socket.send_multipart([b"rank", resposta.SerializeToString()])

    elif tipo == b"list":
        req = mensagem_pb2.ListaServidoresRequest()
        req.ParseFromString(dados)
        atualizar_relogio(req.relogio)
        pausa()
        print(f"[RECEBIDO] list | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)

        resposta = mensagem_pb2.ListaServidoresResponse()
        for nome in sorted(ativos, key=lambda n: ranks[n]):
            info = resposta.servidores.add()
            info.nome = nome
            info.rank = ranks[nome]
        resposta.timestamp = agora()
        resposta.relogio = proximo_relogio()

        lista = [f"{s.nome}:{s.rank}" for s in resposta.servidores]
        print(f"[ENVIADO] list | servidores={lista} | timestamp={fmt(resposta.timestamp)} | relogio={resposta.relogio}", flush=True)
        pausa()
        socket.send_multipart([b"list", resposta.SerializeToString()])

    elif tipo == b"heartbeat":
        req = mensagem_pb2.HeartbeatRequest()
        req.ParseFromString(dados)
        atualizar_relogio(req.relogio)
        print(f"[RECEBIDO] heartbeat | nome={req.nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)

        resposta = mensagem_pb2.HeartbeatResponse()
        resposta.ok = req.nome.strip() != ""
        if resposta.ok:
            registrar(req.nome)
        resposta.relogio = proximo_relogio()

        print(f"[ENVIADO] heartbeat | ok={resposta.ok} | relogio={resposta.relogio}", flush=True)
        socket.send_multipart([b"heartbeat", resposta.SerializeToString()])

    else:
        print(f"tipo de mensagem desconhecido: {tipo}", flush=True)
        socket.send_multipart([tipo, b""])