import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZFrame;

import com.google.protobuf.InvalidProtocolBufferException;

// mvn exec:java "-Dexec.mainClass=ClienteCriarCanal"

public class ClienteCriarCanal {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://localhost:5556");

            Mensagem.CriarCanalRequest canalReq = Mensagem.CriarCanalRequest.newBuilder()
                    .setNomeCanal("geral-java")
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            ZMsg msg = new ZMsg();
            msg.add("criar_canal");
            msg.add(canalReq.toByteArray());
            msg.send(socket);

            ZMsg respostaMsg = ZMsg.recvMsg(socket);
            respostaMsg.popString();
            ZFrame dadosFrame = respostaMsg.pop();

            Mensagem.CriarCanalResponse resposta = Mensagem.CriarCanalResponse.parseFrom(dadosFrame.getData());

            System.out.println("sucesso: " + resposta.getSucesso());
            System.out.println("erro: " + resposta.getErro());
            System.out.println("timestamp: " + resposta.getTimestamp());
        }
    }
}