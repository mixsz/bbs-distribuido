import zmq
import mensagem_pb2

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

print("servidor rodando na porta 5555...", flush=True)

canais = []

while True:
    tipo, dados = socket.recv_multipart()

    if tipo == b"login":
        login_req = mensagem_pb2.LoginRequest()
        login_req.ParseFromString(dados)
        print(f"login recebido: {login_req.usuario}", flush=True)

        resposta = mensagem_pb2.LoginResponse()
        if login_req.usuario.strip() == "":
            resposta.sucesso = False
            resposta.erro = "nome usuario vazio"
        else:
            resposta.sucesso = True
            resposta.erro = ""
        resposta.timestamp = login_req.timestamp

        socket.send_multipart([b"login", resposta.SerializeToString()])

    elif tipo == b"criar_canal":
        canal_req = mensagem_pb2.CriarCanalRequest()
        canal_req.ParseFromString(dados)
        print(f"canal a ser criado: {canal_req.nome_canal}", flush=True)

        resposta = mensagem_pb2.CriarCanalResponse()
        if canal_req.nome_canal.strip() == "":
            resposta.sucesso = False
            resposta.erro = "nome de canal vazio"
        elif canal_req.nome_canal in canais:
            resposta.sucesso = False
            resposta.erro = "canal ja existe"
        else:
            canais.append(canal_req.nome_canal)
            resposta.sucesso = True
            resposta.erro = ""
        resposta.timestamp = canal_req.timestamp

        socket.send_multipart([b"criar_canal", resposta.SerializeToString()])

    elif tipo == b"listar_canais":
        listar_req = mensagem_pb2.ListarCanaisRequest()
        listar_req.ParseFromString(dados)
        print("listar canais recebido", flush=True)

        resposta = mensagem_pb2.ListarCanaisResponse()
        resposta.canais.extend(canais)
        resposta.timestamp = listar_req.timestamp

        socket.send_multipart([b"listar_canais", resposta.SerializeToString()])