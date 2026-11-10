import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Proxy {

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket xsub = context.createSocket(SocketType.XSUB);
            xsub.bind("tcp://*:5557");

            ZMQ.Socket xpub = context.createSocket(SocketType.XPUB);
            xpub.bind("tcp://*:5558");

            System.out.println("proxy pub/sub rodando: servidores publicam na porta 5557, bots assinam na porta 5558");

            ZMQ.proxy(xsub, xpub, null);
        }
    }
}
