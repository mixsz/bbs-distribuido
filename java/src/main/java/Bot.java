import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Bot {
    static final String ENDERECO = System.getenv().getOrDefault("SERVIDOR_ENDERECO", "tcp://localhost:5555");
    static final String USUARIO = System.getenv().getOrDefault("USUARIO", "bot-java");
    static final String CANAL = System.getenv().getOrDefault("CANAL", "geral");
    static final int TIMEOUT_MS = 3000;
    static final int TENTATIVAS = 10;
    static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    static ZContext context = new ZContext();
    static ZMQ.Socket socket = novoSocket();

    static ZMQ.Socket novoSocket() {
        ZMQ.Socket s = context.createSocket(SocketType.REQ);
        s.setReceiveTimeOut(TIMEOUT_MS);
        s.setLinger(0);
        s.connect(ENDERECO);
        return s;
    }

    static String fmt(long ts) {
        return FMT.format(Instant.ofEpochMilli(ts));
    }

    static byte[] enviar(String tipo, byte[] payload) {
        for (int t = 1; t <= TENTATIVAS; t++) {
            ZMsg msg = new ZMsg();
            msg.add(tipo);
            msg.add(payload);
            msg.send(socket);

            ZMsg resposta = ZMsg.recvMsg(socket);
            if (resposta != null) {
                resposta.popString();
                return resposta.pop().getData();
            }
            System.out.println("servidor sem resposta, tentativa " + t + "/" + TENTATIVAS);
            socket = novoSocket();
        }
        System.out.println("servidor nao respondeu, encerrando");
        System.exit(1);
        return null;
    }

    public static void main(String[] args) throws Exception {

        // 1. login
        Mensagem.LoginRequest loginReq = Mensagem.LoginRequest.newBuilder()
                .setUsuario(USUARIO)
                .setTimestamp(System.currentTimeMillis())
                .build();
        System.out.println("[ENVIADO] login | usuario=" + loginReq.getUsuario()
                + " | timestamp=" + fmt(loginReq.getTimestamp()));
        Mensagem.LoginResponse loginResp =
                Mensagem.LoginResponse.parseFrom(enviar("login", loginReq.toByteArray()));
        System.out.println("[RECEBIDO] login | sucesso=" + loginResp.getSucesso()
                + " | erro='" + loginResp.getErro()
                + "' | timestamp=" + fmt(loginResp.getTimestamp()));
        Thread.sleep(1000);

        // 2. criar canal
        Mensagem.CriarCanalRequest canalReq = Mensagem.CriarCanalRequest.newBuilder()
                .setNomeCanal(CANAL)
                .setTimestamp(System.currentTimeMillis())
                .build();
        System.out.println("[ENVIADO] criar_canal | nome_canal=" + canalReq.getNomeCanal()
                + " | timestamp=" + fmt(canalReq.getTimestamp()));
        Mensagem.CriarCanalResponse canalResp =
                Mensagem.CriarCanalResponse.parseFrom(enviar("criar_canal", canalReq.toByteArray()));
        System.out.println("[RECEBIDO] criar_canal | sucesso=" + canalResp.getSucesso()
                + " | erro='" + canalResp.getErro()
                + "' | timestamp=" + fmt(canalResp.getTimestamp()));
        Thread.sleep(1000);

        // 3. listar canais
        Mensagem.ListarCanaisRequest listarReq = Mensagem.ListarCanaisRequest.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .build();
        System.out.println("[ENVIADO] listar_canais | timestamp=" + fmt(listarReq.getTimestamp()));
        Mensagem.ListarCanaisResponse listarResp =
                Mensagem.ListarCanaisResponse.parseFrom(enviar("listar_canais", listarReq.toByteArray()));
        System.out.println("[RECEBIDO] listar_canais | canais=" + listarResp.getCanaisList()
                + " | timestamp=" + fmt(listarResp.getTimestamp()));

        context.close();
    }
}