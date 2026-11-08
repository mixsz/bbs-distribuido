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
PROXY_SUB = os.environ.get("PROXY_SUB_ENDERECO", "tcp://proxy:5558")
PEER_PORTA = os.environ.get("PEER_PORTA", "5600")
PEER_BIND = os.environ.get("PEER_BIND", f"tcp://*:{PEER_PORTA}")
TOPICO_SERVERS = "servers"
TOPICO_REPLICACAO = "replicacao"

INTERVALO_HEARTBEAT = 10
INTERVALO_SINCRONIZACAO = 15
INTERVALO_REPLICACAO = 20
TIMEOUT_MS = 3000
TIMEOUT_PAR_MS = 2000
TENTATIVAS = 20

relogio = 0
desvio = int(os.environ.get("DESVIO_RELOGIO_MS", "0"))
mensagens_clientes = 0
meu_rank = 0
coordenador = None
eleicao_pendente = False
primeiro_anuncio = True
TTL_LISTA_S = 3
cache_lista = []
cache_hora = 0.0

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

def listar_servidores(usar_cache=False):
    global cache_lista, cache_hora
    if usar_cache and time.time() - cache_hora < TTL_LISTA_S:
        return cache_lista
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
    cache_lista = [(s.nome, s.rank) for s in resp.servidores]
    cache_hora = time.time()
    return cache_lista

def heartbeat():
    req = mensagem_pb2.HeartbeatRequest()
    req.nome = NOME
    req.timestamp = agora()
    req.relogio = proximo_relogio()
    print(f"[ENVIADO] heartbeat | nome={req.nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)

    _, dados = pedir_referencia(b"heartbeat", req.SerializeToString())

    resp = mensagem_pb2.HeartbeatResponse()
    resp.ParseFromString(dados)
    atualizar_relogio(resp.relogio)
    print(f"[RECEBIDO] heartbeat | ok={resp.ok} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio} | local={relogio}", flush=True)
    pausa()


def pedir_servidor(nome, tipo, payload):
    global eleicao_pendente
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.SNDTIMEO, TIMEOUT_PAR_MS)
    s.setsockopt(zmq.LINGER, 0)
    try:
        s.connect(f"tcp://{nome}:{PEER_PORTA}")
        s.send_multipart([tipo, payload])
        espera = zmq.Poller()
        espera.register(s, zmq.POLLIN)
        espera.register(peer, zmq.POLLIN)
        fim = time.time() + TIMEOUT_PAR_MS / 1000
        while True:
            restante = int((fim - time.time()) * 1000)
            if restante <= 0:
                return None
            eventos = dict(espera.poll(restante))
            if peer in eventos:
                if atender_par():
                    eleicao_pendente = True
                continue
            if s in eventos:
                _, resposta = s.recv_multipart()
                return resposta
    except zmq.ZMQError:
        return None
    finally:
        s.close()


def definir_coordenador(nome):
    global coordenador
    coordenador = nome
    print(f"[COORDENADOR] coordenador={coordenador}", flush=True)


def anunciar_coordenador():
    global primeiro_anuncio
    # espera um pouco por causa do pub/proxy (so no primeiro anuncio quando a conexao e nova tb)
    if primeiro_anuncio:
        time.sleep(1)
        primeiro_anuncio = False
    anuncio = mensagem_pb2.CoordenadorAnuncio()
    anuncio.nome = NOME
    anuncio.timestamp = agora()
    anuncio.relogio = proximo_relogio()
    pub.send_multipart([TOPICO_SERVERS.encode(), anuncio.SerializeToString()])
    print(f"[PUBLICADO] topico={TOPICO_SERVERS} | coordenador={anuncio.nome} | timestamp={fmt(anuncio.timestamp)} | relogio={anuncio.relogio}", flush=True)


def iniciar_eleicao():
    global coordenador
    coordenador = None
    print(f"[ELEICAO] iniciada por {NOME} (rank={meu_rank})", flush=True)
    superiores = sorted([(n, r) for n, r in listar_servidores(usar_cache=True) if r > meu_rank], key=lambda x: x[1])

    alguem_respondeu = False
    for nome, rank in superiores:
        req = mensagem_pb2.EleicaoRequest()
        req.nome = NOME
        req.rank = meu_rank
        req.timestamp = agora()
        req.relogio = proximo_relogio()
        print(f"[ENVIADO] eleicao | para={nome} | rank={req.rank} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)
        dados = pedir_servidor(nome, b"eleicao", req.SerializeToString())
        if dados is None:
            print(f"[ELEICAO] {nome} nao respondeu", flush=True)
            continue
        resp = mensagem_pb2.EleicaoResponse()
        resp.ParseFromString(dados)
        atualizar_relogio(resp.relogio)
        print(f"[RECEBIDO] eleicao | de={nome} | resposta={resp.resposta} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio} | local={relogio}", flush=True)
        alguem_respondeu = True

    if alguem_respondeu:
        # um servidor de rank maior ta vivo = eleicao continua
        print(f"[ELEICAO] {NOME} aguarda o anuncio do coordenador", flush=True)
        return

    definir_coordenador(NOME)
    anunciar_coordenador()


def sincronizar_relogio():
    global desvio
    if coordenador is None:
        iniciar_eleicao()
        return
    if coordenador == NOME:
        print(f"[SINCRONIZACAO] {NOME} e o coordenador, mantem o proprio relogio", flush=True)
        return

    req = mensagem_pb2.RelogioRequest()
    req.nome = NOME
    req.timestamp = agora()
    req.relogio = proximo_relogio()
    print(f"[ENVIADO] relogio | para={coordenador} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)

    t0 = agora_sistema()
    dados = pedir_servidor(coordenador, b"relogio", req.SerializeToString())
    t1 = agora_sistema()

    if dados is None:
        print(f"[COORDENADOR] {coordenador} indisponivel", flush=True)
        iniciar_eleicao()
        return

    resp = mensagem_pb2.RelogioResponse()
    resp.ParseFromString(dados)
    atualizar_relogio(resp.relogio)
    print(f"[RECEBIDO] relogio | de={coordenador} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio} | local={relogio}", flush=True)

    estimada = resp.timestamp + (t1 - t0) // 2
    novo_desvio = estimada - t1
    ajuste = novo_desvio - desvio
    desvio = novo_desvio
    print(f"[SINCRONIZACAO] coordenador={coordenador} | hora={fmt(resp.timestamp)} | rtt={t1 - t0}ms | ajuste={ajuste:+d}ms | desvio={desvio:+d}ms", flush=True)
    pausa()


def atender_par():
    tipo, msg_dados = peer.recv_multipart()
    if tipo == b"relogio":
        req = mensagem_pb2.RelogioRequest()
        req.ParseFromString(msg_dados)
        atualizar_relogio(req.relogio)
        print(f"[RECEBIDO] relogio | de={req.nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio} | local={relogio}", flush=True)
        resp = mensagem_pb2.RelogioResponse()
        resp.timestamp = agora()
        resp.relogio = proximo_relogio()
        print(f"[ENVIADO] relogio | para={req.nome} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio}", flush=True)
        peer.send_multipart([b"relogio", resp.SerializeToString()])
    elif tipo == b"eleicao":
        req = mensagem_pb2.EleicaoRequest()
        req.ParseFromString(msg_dados)
        atualizar_relogio(req.relogio)
        print(f"[RECEBIDO] eleicao | de={req.nome} | rank={req.rank} | timestamp={fmt(req.timestamp)} | relogio={req.relogio} | local={relogio}", flush=True)
        resp = mensagem_pb2.EleicaoResponse()
        resp.resposta = "OK"
        resp.timestamp = agora()
        resp.relogio = proximo_relogio()
        print(f"[ENVIADO] eleicao | para={req.nome} | resposta={resp.resposta} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio}", flush=True)
        peer.send_multipart([b"eleicao", resp.SerializeToString()])
        return req.rank < meu_rank  # qm tem rank maior assume
    elif tipo == b"dados":
        req = mensagem_pb2.DadosRequest()
        req.ParseFromString(msg_dados)
        atualizar_relogio(req.relogio)
        print(f"[RECEBIDO] dados | de={req.nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio} | local={relogio}", flush=True)
        resp = montar_dados_response()
        print(f"[ENVIADO] dados | para={req.nome} | logins={len(resp.logins)} canais={len(resp.canais)} publicacoes={len(resp.publicacoes)} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio}", flush=True)
        peer.send_multipart([b"dados", resp.SerializeToString()])
    else:
        print(f"tipo de mensagem desconhecido (par): {tipo}", flush=True)
        peer.send_multipart([tipo, b""])


chaves_logins = set()
chaves_publicacoes = set()


def indexar_dados():
    chaves_logins.clear()
    chaves_publicacoes.clear()
    for l in dados["logins"]:
        chaves_logins.add((l["usuario"], l["timestamp"]))
    for p in dados["publicacoes"]:
        chaves_publicacoes.add((p["canal"], p["mensagem"], p["usuario"], p["timestamp"]))

def gravar_login(usuario, timestamp):
    chave = (usuario, timestamp)
    if chave in chaves_logins:
        return False
    chaves_logins.add(chave)
    dados["logins"].append({"usuario": usuario, "timestamp": timestamp})
    return True

def gravar_canal(nome_canal):
    if nome_canal in dados["canais"]:
        return False
    dados["canais"].append(nome_canal)
    return True

def ordenar_publicacoes():
    dados["publicacoes"].sort(key=lambda x: x["timestamp"])

def gravar_publicacao(canal, mensagem, usuario, timestamp, ordenar=True):
    chave = (canal, mensagem, usuario, timestamp)
    if chave in chaves_publicacoes:
        return False
    chaves_publicacoes.add(chave)
    dados["publicacoes"].append({"canal": canal, "mensagem": mensagem, "usuario": usuario, "timestamp": timestamp})
    if ordenar:
        ordenar_publicacoes()
    return True

def replicar(login=None, canal=None, publicacao=None):
    msg = mensagem_pb2.Replicacao()
    msg.origem = NOME
    msg.timestamp = agora()
    msg.relogio = proximo_relogio()
    if login is not None:
        msg.login.usuario, msg.login.timestamp = login
        detalhe = f"tipo=login | usuario={login[0]}"
    elif canal is not None:
        msg.canal = canal
        detalhe = f"tipo=canal | canal={canal}"
    else:
        msg.publicacao.canal, msg.publicacao.mensagem, msg.publicacao.usuario, msg.publicacao.timestamp = publicacao
        detalhe = f"tipo=publicacao | canal={publicacao[0]} | mensagem='{publicacao[1]}'"
    pub.send_multipart([TOPICO_REPLICACAO.encode(), msg.SerializeToString()])
    print(f"[REPLICADO] {detalhe} | timestamp={fmt(msg.timestamp)} | relogio={msg.relogio}", flush=True)

def aplicar_replicacao(msg_dados):
    rep = mensagem_pb2.Replicacao()
    rep.ParseFromString(msg_dados)
    atualizar_relogio(rep.relogio)
    if rep.origem == NOME:
        return  # o proprio servidor tambem recebe o que publicou
    tipo = rep.WhichOneof("dado")
    if tipo == "login":
        novo = gravar_login(rep.login.usuario, rep.login.timestamp)
        detalhe = f"tipo=login | usuario={rep.login.usuario}"
    elif tipo == "canal":
        novo = gravar_canal(rep.canal)
        detalhe = f"tipo=canal | canal={rep.canal}"
    elif tipo == "publicacao":
        p = rep.publicacao
        novo = gravar_publicacao(p.canal, p.mensagem, p.usuario, p.timestamp)
        detalhe = f"tipo=publicacao | canal={p.canal} | mensagem='{p.mensagem}'"
    else:
        return
    if novo:
        salvar_dados(dados)
    print(f"[RECEBIDO] replicacao | origem={rep.origem} | {detalhe} | novo={novo} | timestamp={fmt(rep.timestamp)} | relogio={rep.relogio} | local={relogio}", flush=True)

def montar_dados_response():
    resp = mensagem_pb2.DadosResponse()
    for l in dados["logins"]:
        resp.logins.add(usuario=l["usuario"], timestamp=l["timestamp"])
    resp.canais.extend(dados["canais"])
    for p in dados["publicacoes"]:
        resp.publicacoes.add(canal=p["canal"], mensagem=p["mensagem"], usuario=p["usuario"], timestamp=p["timestamp"])
    resp.timestamp = agora()
    resp.relogio = proximo_relogio()
    return resp

def sincronizar_dados():
    for nome, _ in listar_servidores(usar_cache=True):
        if nome == NOME:
            continue
        req = mensagem_pb2.DadosRequest()
        req.nome = NOME
        req.timestamp = agora()
        req.relogio = proximo_relogio()
        print(f"[ENVIADO] dados | para={nome} | timestamp={fmt(req.timestamp)} | relogio={req.relogio}", flush=True)
        resposta_bytes = pedir_servidor(nome, b"dados", req.SerializeToString())
        if resposta_bytes is None:
            print(f"[REPLICACAO] {nome} nao respondeu ao pedido de dados", flush=True)
            continue
        resp = mensagem_pb2.DadosResponse()
        resp.ParseFromString(resposta_bytes)
        atualizar_relogio(resp.relogio)
        novos_logins = sum(gravar_login(l.usuario, l.timestamp) for l in resp.logins)
        novos_canais = sum(gravar_canal(c) for c in resp.canais)
        novas_publicacoes = sum(gravar_publicacao(p.canal, p.mensagem, p.usuario, p.timestamp, ordenar=False) for p in resp.publicacoes)
        if novos_logins or novos_canais or novas_publicacoes:
            ordenar_publicacoes()
            salvar_dados(dados)
        print(f"[RECEBIDO] dados | de={nome} | recebidos: logins={len(resp.logins)} canais={len(resp.canais)} publicacoes={len(resp.publicacoes)} | novos aqui: logins={novos_logins} canais={novos_canais} publicacoes={novas_publicacoes} | timestamp={fmt(resp.timestamp)} | relogio={resp.relogio} | local={relogio}", flush=True)

def atender_anuncio():
    topico, msg_dados = sub.recv_multipart()
    if topico == TOPICO_REPLICACAO.encode():
        aplicar_replicacao(msg_dados)
        return
    anuncio = mensagem_pb2.CoordenadorAnuncio()
    anuncio.ParseFromString(msg_dados)
    atualizar_relogio(anuncio.relogio)
    print(f"[RECEBIDO] topico={TOPICO_SERVERS} | coordenador={anuncio.nome} | timestamp={fmt(anuncio.timestamp)} | relogio={anuncio.relogio} | local={relogio}", flush=True)
    era_coordenador = coordenador == NOME
    definir_coordenador(anuncio.nome)
    if anuncio.nome != NOME:
        rank_anunciado = dict(listar_servidores(usar_cache=True)).get(anuncio.nome)
        if rank_anunciado is not None and rank_anunciado < meu_rank:
            if era_coordenador:
                print(f"[ELEICAO] {anuncio.nome} tem rank menor que {NOME}, reafirmando coordenacao", flush=True)
                definir_coordenador(NOME)
                anunciar_coordenador()
            else:
                print(f"[ELEICAO] {anuncio.nome} tem rank menor que {NOME}, nova eleicao", flush=True)
                iniciar_eleicao()

rank = obter_rank()
meu_rank = rank
print(f"servidor {NOME} cadastrado com rank {rank}", flush=True)
listar_servidores()

socket = context.socket(zmq.REP)
socket.connect(BROKER)

pub = context.socket(zmq.PUB)
pub.connect(PROXY_PUB)

peer = context.socket(zmq.REP)
peer.bind(PEER_BIND)

sub = context.socket(zmq.SUB)
sub.connect(PROXY_SUB)
sub.setsockopt(zmq.SUBSCRIBE, TOPICO_SERVERS.encode())
sub.setsockopt(zmq.SUBSCRIBE, TOPICO_REPLICACAO.encode())

poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)
poller.register(peer, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

print(f"servidor python conectado ao broker em {BROKER}, ao proxy em {PROXY_PUB}/{PROXY_SUB} e ouvindo servidores em {PEER_BIND}", flush=True)

dados = carregar_dados()
indexar_dados()

iniciar_eleicao()
sincronizar_dados()

while True:
    eventos = dict(poller.poll(1000))
    if sub in eventos:
        atender_anuncio()
        while sub.poll(0):  # le tudo o que ja chegou no SUB
            atender_anuncio()
    if peer in eventos:
        if atender_par():
            eleicao_pendente = True
    if eleicao_pendente:
        iniciar_eleicao()
        eleicao_pendente = False
    if socket not in eventos:
        continue

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
            gravar_login(login_req.usuario, login_req.timestamp)
            salvar_dados(dados)
            replicar(login=(login_req.usuario, login_req.timestamp))
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
            gravar_canal(canal_req.nome_canal)
            salvar_dados(dados)
            replicar(canal=canal_req.nome_canal)
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

            gravar_publicacao(publicacao.canal, publicacao.mensagem, publicacao.usuario, publicacao.timestamp)
            salvar_dados(dados)
            replicar(publicacao=(publicacao.canal, publicacao.mensagem, publicacao.usuario, publicacao.timestamp))
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

    if mensagens_clientes % INTERVALO_SINCRONIZACAO == 0:
        sincronizar_relogio()

    if mensagens_clientes % INTERVALO_REPLICACAO == 0:
        sincronizar_dados()