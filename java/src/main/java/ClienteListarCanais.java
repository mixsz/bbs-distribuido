import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZFrame;

import com.google.protobuf.InvalidProtocolBufferException;

// mvn exec:java "-Dexec.mainClass=ClienteListarCanais"

public class ClienteListarCanais {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://localhost:5556");

            Mensagem.ListarCanaisRequest listarReq = Mensagem.ListarCanaisRequest.newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            ZMsg msg = new ZMsg();
            msg.add("listar_canais");
            msg.add(listarReq.toByteArray());
            msg.send(socket);

            ZMsg respostaMsg = ZMsg.recvMsg(socket);
            respostaMsg.popString();
            ZFrame dadosFrame = respostaMsg.pop();

            Mensagem.ListarCanaisResponse resposta = Mensagem.ListarCanaisResponse.parseFrom(dadosFrame.getData());

            System.out.println("canais: " + resposta.getCanaisList());
            System.out.println("timestamp: " + resposta.getTimestamp());
        }
    }
}