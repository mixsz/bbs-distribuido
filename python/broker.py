import zmq

context = zmq.Context()

frontend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")

backend = context.socket(zmq.DEALER)
backend.bind("tcp://*:5556")

print("broker rodando: bots na porta 5555, servidores na porta 5556", flush=True)

zmq.proxy(frontend, backend)