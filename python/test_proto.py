import mensagem_pb2

msg = mensagem_pb2.LoginRequest()
msg.usuario = "testsetetestestst"
msg.timestamp = 1234567890

dados = msg.SerializeToString()
print("Bytes gerados:", dados)

msg_recebida = mensagem_pb2.LoginRequest()
msg_recebida.ParseFromString(dados)
print("Usuario recebido:", msg_recebida.usuario)
print("Timestamp recebido:", msg_recebida.timestamp)