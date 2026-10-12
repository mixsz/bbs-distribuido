import zmq

context = zmq.Context()

xsub = context.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

xpub = context.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("proxy pub/sub rodando: servidores publicam na porta 5557, bots assinam na porta 5558", flush=True)

zmq.proxy(xsub, xpub)