import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Broker {

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket frontend = context.createSocket(SocketType.ROUTER);
            frontend.bind("tcp://*:5555");

            ZMQ.Socket backend = context.createSocket(SocketType.DEALER);
            backend.bind("tcp://*:5556");

            System.out.println("broker rodando: bots na porta 5555, servidores na porta 5556");

            ZMQ.proxy(frontend, backend, null);
        }
    }
}
