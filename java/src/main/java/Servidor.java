import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import org.zeromq.ZFrame;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

// mvn exec:java "-Dexec.mainClass=Servidor"

public class Servidor {
    private static final String ARQUIVO_DADOS = System.getenv().getOrDefault("DADOS_PATH", "dados.json");
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    static String fmt(long ts) {
        return FMT.format(Instant.ofEpochMilli(ts));
    }

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            String broker = System.getenv().getOrDefault("BROKER_ENDERECO", "tcp://localhost:5556");
            socket.connect(broker);

            System.out.println("servidor java conectado ao broker em " + broker);

            Dados dados = carregarDados();

            while (!Thread.currentThread().isInterrupted()) {
                ZMsg msg = ZMsg.recvMsg(socket);
                String tipo = msg.popString();
                ZFrame dadosFrame = msg.pop();
                byte[] dadosBytes = dadosFrame.getData();

                if (tipo.equals("login")) {
                    try {
                        Mensagem.LoginRequest loginReq = Mensagem.LoginRequest.parseFrom(dadosBytes);
                        pausa();
                        System.out.println("[RECEBIDO] login | usuario=" + loginReq.getUsuario()
                                + " | timestamp=" + fmt(loginReq.getTimestamp()));

                        Mensagem.LoginResponse.Builder resposta = Mensagem.LoginResponse.newBuilder();
                        if (loginReq.getUsuario().trim().isEmpty()) {
                            resposta.setSucesso(false);
                            resposta.setErro("nome usuario vazio");
                        } else {
                            resposta.setSucesso(true);
                            resposta.setErro("");
                            dados.logins.add(new Dados.LoginSalvo(loginReq.getUsuario(), loginReq.getTimestamp()));
                            salvarDados(dados);
                        }
                        resposta.setTimestamp(loginReq.getTimestamp());

                        System.out.println("[ENVIADO] login | sucesso=" + resposta.getSucesso()
                                + " | erro='" + resposta.getErro()
                                + "' | timestamp=" + fmt(resposta.getTimestamp()));
                        pausa();

                        ZMsg respostaMsg = new ZMsg();
                        respostaMsg.add("login");
                        respostaMsg.add(resposta.build().toByteArray());
                        respostaMsg.send(socket);

                    } catch (InvalidProtocolBufferException e) {
                        System.out.println("erro ao desserializar: " + e.getMessage());
                    }

                } else if (tipo.equals("criar_canal")) {
                    try {
                        Mensagem.CriarCanalRequest canalReq = Mensagem.CriarCanalRequest.parseFrom(dadosBytes);
                        pausa();
                        System.out.println("[RECEBIDO] criar_canal | nome_canal=" + canalReq.getNomeCanal()
                                + " | timestamp=" + fmt(canalReq.getTimestamp()));

                        Mensagem.CriarCanalResponse.Builder resposta = Mensagem.CriarCanalResponse.newBuilder();
                        if (canalReq.getNomeCanal().trim().isEmpty()) {
                            resposta.setSucesso(false);
                            resposta.setErro("nome de canal vazio");
                        } else if (dados.canais.contains(canalReq.getNomeCanal())) {
                            resposta.setSucesso(false);
                            resposta.setErro("canal ja existe");
                        } else {
                            dados.canais.add(canalReq.getNomeCanal());
                            salvarDados(dados);
                            resposta.setSucesso(true);
                            resposta.setErro("");
                        }
                        resposta.setTimestamp(canalReq.getTimestamp());

                        System.out.println("[ENVIADO] criar_canal | sucesso=" + resposta.getSucesso()
                                + " | erro='" + resposta.getErro()
                                + "' | timestamp=" + fmt(resposta.getTimestamp()));
                        pausa();

                        ZMsg respostaMsg = new ZMsg();
                        respostaMsg.add("criar_canal");
                        respostaMsg.add(resposta.build().toByteArray());
                        respostaMsg.send(socket);

                    } catch (InvalidProtocolBufferException e) {
                        System.out.println("erro ao desserializar: " + e.getMessage());
                    }

                } else if (tipo.equals("listar_canais")) {
                    try {
                        Mensagem.ListarCanaisRequest listarReq = Mensagem.ListarCanaisRequest.parseFrom(dadosBytes);
                        pausa();
                        System.out.println("[RECEBIDO] listar_canais | timestamp=" + fmt(listarReq.getTimestamp()));

                        Mensagem.ListarCanaisResponse.Builder resposta = Mensagem.ListarCanaisResponse.newBuilder();
                        resposta.addAllCanais(dados.canais);
                        resposta.setTimestamp(listarReq.getTimestamp());

                        System.out.println("[ENVIADO] listar_canais | canais=" + resposta.getCanaisList()
                                + " | timestamp=" + fmt(resposta.getTimestamp()));
                        pausa();

                        ZMsg respostaMsg = new ZMsg();
                        respostaMsg.add("listar_canais");
                        respostaMsg.add(resposta.build().toByteArray());
                        respostaMsg.send(socket);

                    } catch (InvalidProtocolBufferException e) {
                        System.out.println("erro ao desserializar: " + e.getMessage());
                    }
                }
            }
        }
    }

    private static Dados carregarDados() {
        File arquivo = new File(ARQUIVO_DADOS);
        if (arquivo.exists()) {
            try (FileReader reader = new FileReader(arquivo)) {
                return gson.fromJson(reader, Dados.class);
            } catch (IOException e) {
                System.out.println("erro ao carregar dados: " + e.getMessage());
            }
        }
        return new Dados();
    }

    private static void salvarDados(Dados dados) {
        try (FileWriter writer = new FileWriter(ARQUIVO_DADOS)) {
            gson.toJson(dados, writer);
        } catch (IOException e) {
            System.out.println("erro ao salvar dados: " + e.getMessage());
        }
    }

    static void pausa() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}