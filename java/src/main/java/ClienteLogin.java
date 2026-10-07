import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZFrame;

import com.google.protobuf.InvalidProtocolBufferException;

// mvn exec:java "-Dexec.mainClass=ClienteLogin"

public class ClienteLogin {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect("tcp://localhost:5556");

            Mensagem.LoginRequest loginReq = Mensagem.LoginRequest.newBuilder()
                    .setUsuario("danVjava")
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            ZMsg msg = new ZMsg();
            msg.add("login");
            msg.add(loginReq.toByteArray());
            msg.send(socket);

            ZMsg respostaMsg = ZMsg.recvMsg(socket);
            String tipo = respostaMsg.popString();
            ZFrame dadosFrame = respostaMsg.pop();

            Mensagem.LoginResponse resposta = Mensagem.LoginResponse.parseFrom(dadosFrame.getData());

            System.out.println("sucesso: " + resposta.getSucesso());
            System.out.println("erro: " + resposta.getErro());
            System.out.println("timestamp: " + resposta.getTimestamp());
        }
    }
}