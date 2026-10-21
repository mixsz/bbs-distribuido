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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

// mvn exec:java "-Dexec.mainClass=Servidor"

public class Servidor {
    static final String ARQUIVO_DADOS = System.getenv().getOrDefault("DADOS_PATH", "dados.json");
    static final String NOME = System.getenv().getOrDefault("NOME_SERVIDOR", "servidor-java");
    static final String REFERENCIA = System.getenv().getOrDefault("REFERENCIA_ENDERECO", "tcp://localhost:5559");
    static final long PAUSA_MS = Long.parseLong(System.getenv().getOrDefault("PAUSA_MS", "200"));

    static final int INTERVALO_HEARTBEAT = 10;
    static final int TIMEOUT_MS = 3000;
    static final int TENTATIVAS = 20;

    static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    static long relogio = 0;
    static long desvio = Long.parseLong(System.getenv().getOrDefault("DESVIO_RELOGIO_MS", "0"));
    static int mensagensClientes = 0;

    static ZContext context;
    static ZMQ.Socket ref;

    static long agoraSistema() {
        return System.currentTimeMillis();
    }

    // relogio fisico do servidor: relogio do sistema + desvio
    static long agora() {
        return agoraSistema() + desvio;
    }

    static String fmt(long ts) {
        return FMT.format(Instant.ofEpochMilli(ts));
    }

    static void pausa() {
        if (PAUSA_MS <= 0) {
            return;
        }
        try {
            Thread.sleep(PAUSA_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static long proximoRelogio() {
        relogio += 1;
        return relogio;
    }

    static void atualizarRelogio(long recebido) {
        relogio = Math.max(relogio, recebido);
    }

    static ZMQ.Socket novoSocketReferencia() {
        ZMQ.Socket s = context.createSocket(SocketType.REQ);
        s.setReceiveTimeOut(TIMEOUT_MS);
        s.setLinger(0);
        s.connect(REFERENCIA);
        return s;
    }

    static byte[] pedirReferencia(String tipo, byte[] payload) {
        for (int t = 1; t <= TENTATIVAS; t++) {
            ZMsg msg = new ZMsg();
            msg.add(tipo);
            msg.add(payload);
            msg.send(ref);

            ZMsg resposta = ZMsg.recvMsg(ref);
            if (resposta != null) {
                resposta.popString();
                return resposta.pop().getData();
            }
            System.out.println("referencia sem resposta, tentativa " + t + "/" + TENTATIVAS);
            ref = novoSocketReferencia();
        }
        System.out.println("referencia nao respondeu, encerrando");
        System.exit(1);
        return null;
    }

    static int obterRank() throws InvalidProtocolBufferException {
        Mensagem.RankRequest req = Mensagem.RankRequest.newBuilder()
                .setNome(NOME)
                .setTimestamp(agora())
                .setRelogio(proximoRelogio())
                .build();
        System.out.println("[ENVIADO] rank | nome=" + req.getNome()
                + " | timestamp=" + fmt(req.getTimestamp())
                + " | relogio=" + req.getRelogio());
        Mensagem.RankResponse resp = Mensagem.RankResponse.parseFrom(pedirReferencia("rank", req.toByteArray()));
        atualizarRelogio(resp.getRelogio());
        System.out.println("[RECEBIDO] rank | rank=" + resp.getRank()
                + " | timestamp=" + fmt(resp.getTimestamp())
                + " | relogio=" + resp.getRelogio()
                + " | local=" + relogio);
        return resp.getRank();
    }

    static void listarServidores() throws InvalidProtocolBufferException {
        Mensagem.ListaServidoresRequest req = Mensagem.ListaServidoresRequest.newBuilder()
                .setTimestamp(agora())
                .setRelogio(proximoRelogio())
                .build();
        System.out.println("[ENVIADO] list | timestamp=" + fmt(req.getTimestamp())
                + " | relogio=" + req.getRelogio());
        Mensagem.ListaServidoresResponse resp =
                Mensagem.ListaServidoresResponse.parseFrom(pedirReferencia("list", req.toByteArray()));
        atualizarRelogio(resp.getRelogio());
        List<String> lista = new ArrayList<>();
        for (Mensagem.ServidorInfo s : resp.getServidoresList()) {
            lista.add(s.getNome() + ":" + s.getRank());
        }
        System.out.println("[RECEBIDO] list | servidores=" + lista
                + " | timestamp=" + fmt(resp.getTimestamp())
                + " | relogio=" + resp.getRelogio()
                + " | local=" + relogio);
    }

    static void heartbeat() throws InvalidProtocolBufferException {
        Mensagem.HeartbeatRequest req = Mensagem.HeartbeatRequest.newBuilder()
                .setNome(NOME)
                .setTimestamp(agora())
                .setRelogio(proximoRelogio())
                .build();
        System.out.println("[ENVIADO] heartbeat | nome=" + req.getNome()
                + " | timestamp=" + fmt(req.getTimestamp())
                + " | relogio=" + req.getRelogio());

        long t0 = agoraSistema();
        byte[] dados = pedirReferencia("heartbeat", req.toByteArray());
        long t1 = agoraSistema();

        Mensagem.HeartbeatResponse resp = Mensagem.HeartbeatResponse.parseFrom(dados);
        atualizarRelogio(resp.getRelogio());
        System.out.println("[RECEBIDO] heartbeat | ok=" + resp.getOk()
                + " | timestamp=" + fmt(resp.getTimestamp())
                + " | relogio=" + resp.getRelogio()
                + " | local=" + relogio);

        // hora correta estimada = hora da referencia + metade do tempo de ida e volta (igual o do py (pra lembrar))
        long estimada = resp.getTimestamp() + (t1 - t0) / 2;
        long novoDesvio = estimada - t1;
        long ajuste = novoDesvio - desvio;
        desvio = novoDesvio;
        System.out.println("[SINCRONIZACAO] referencia=" + fmt(resp.getTimestamp())
                + " | rtt=" + (t1 - t0) + "ms"
                + " | ajuste=" + String.format("%+d", ajuste) + "ms"
                + " | desvio=" + String.format("%+d", desvio) + "ms");
        pausa();
    }

    public static void main(String[] args) throws Exception {
        try (ZContext ctx = new ZContext()) {
            context = ctx;
            ref = novoSocketReferencia();

            int rank = obterRank();
            System.out.println("servidor " + NOME + " cadastrado com rank " + rank);
            listarServidores();

            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            String broker = System.getenv().getOrDefault("BROKER_ENDERECO", "tcp://localhost:5556");
            socket.connect(broker);

            ZMQ.Socket pub = context.createSocket(SocketType.PUB);
            String proxyPub = System.getenv().getOrDefault("PROXY_PUB_ENDERECO", "tcp://localhost:5557");
            pub.connect(proxyPub);

            System.out.println("servidor java conectado ao broker em " + broker + " e ao proxy em " + proxyPub);

            Dados dados = carregarDados();

            while (!Thread.currentThread().isInterrupted()) {
                ZMsg msg = ZMsg.recvMsg(socket);
                String tipo = msg.popString();
                ZFrame dadosFrame = msg.pop();
                byte[] dadosBytes = dadosFrame.getData();
                mensagensClientes++;

                if (tipo.equals("login")) {
                    try {
                        Mensagem.LoginRequest loginReq = Mensagem.LoginRequest.parseFrom(dadosBytes);
                        atualizarRelogio(loginReq.getRelogio());
                        pausa();
                        System.out.println("[RECEBIDO] login | usuario=" + loginReq.getUsuario()
                                + " | timestamp=" + fmt(loginReq.getTimestamp())
                                + " | relogio=" + loginReq.getRelogio()
                                + " | local=" + relogio);

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
                        resposta.setTimestamp(agora());
                        resposta.setRelogio(proximoRelogio());

                        System.out.println("[ENVIADO] login | sucesso=" + resposta.getSucesso()
                                + " | erro='" + resposta.getErro()
                                + "' | timestamp=" + fmt(resposta.getTimestamp())
                                + " | relogio=" + resposta.getRelogio());
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
                        atualizarRelogio(canalReq.getRelogio());
                        pausa();
                        System.out.println("[RECEBIDO] criar_canal | nome_canal=" + canalReq.getNomeCanal()
                                + " | timestamp=" + fmt(canalReq.getTimestamp())
                                + " | relogio=" + canalReq.getRelogio()
                                + " | local=" + relogio);

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
                        resposta.setTimestamp(agora());
                        resposta.setRelogio(proximoRelogio());

                        System.out.println("[ENVIADO] criar_canal | sucesso=" + resposta.getSucesso()
                                + " | erro='" + resposta.getErro()
                                + "' | timestamp=" + fmt(resposta.getTimestamp())
                                + " | relogio=" + resposta.getRelogio());
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
                        atualizarRelogio(listarReq.getRelogio());
                        pausa();
                        System.out.println("[RECEBIDO] listar_canais | timestamp=" + fmt(listarReq.getTimestamp())
                                + " | relogio=" + listarReq.getRelogio()
                                + " | local=" + relogio);

                        Mensagem.ListarCanaisResponse.Builder resposta = Mensagem.ListarCanaisResponse.newBuilder();
                        resposta.addAllCanais(dados.canais);
                        resposta.setTimestamp(agora());
                        resposta.setRelogio(proximoRelogio());

                        System.out.println("[ENVIADO] listar_canais | canais=" + resposta.getCanaisList()
                                + " | timestamp=" + fmt(resposta.getTimestamp())
                                + " | relogio=" + resposta.getRelogio());
                        pausa();

                        ZMsg respostaMsg = new ZMsg();
                        respostaMsg.add("listar_canais");
                        respostaMsg.add(resposta.build().toByteArray());
                        respostaMsg.send(socket);

                    } catch (InvalidProtocolBufferException e) {
                        System.out.println("erro ao desserializar: " + e.getMessage());
                    }

                } else if (tipo.equals("publicar")) {
                    try {
                        Mensagem.PublicarRequest pubReq = Mensagem.PublicarRequest.parseFrom(dadosBytes);
                        atualizarRelogio(pubReq.getRelogio());
                        pausa();
                        System.out.println("[RECEBIDO] publicar | usuario=" + pubReq.getUsuario()
                                + " | canal=" + pubReq.getCanal()
                                + " | mensagem='" + pubReq.getMensagem()
                                + "' | timestamp=" + fmt(pubReq.getTimestamp())
                                + " | relogio=" + pubReq.getRelogio()
                                + " | local=" + relogio);

                        Mensagem.PublicarResponse.Builder resposta = Mensagem.PublicarResponse.newBuilder();
                        if (pubReq.getCanal().trim().isEmpty()) {
                            resposta.setSucesso(false);
                            resposta.setErro("canal vazio");
                        } else if (pubReq.getMensagem().trim().isEmpty()) {
                            resposta.setSucesso(false);
                            resposta.setErro("mensagem vazia");
                        } else {
                            Mensagem.Publicacao publicacao = Mensagem.Publicacao.newBuilder()
                                    .setCanal(pubReq.getCanal())
                                    .setMensagem(pubReq.getMensagem())
                                    .setUsuario(pubReq.getUsuario())
                                    .setTimestamp(agora())
                                    .setRelogio(proximoRelogio())
                                    .build();

                            ZMsg pubMsg = new ZMsg();
                            pubMsg.add(publicacao.getCanal());
                            pubMsg.add(publicacao.toByteArray());
                            pubMsg.send(pub);

                            System.out.println("[PUBLICADO] canal=" + publicacao.getCanal()
                                    + " | mensagem='" + publicacao.getMensagem()
                                    + "' | usuario=" + publicacao.getUsuario()
                                    + " | timestamp=" + fmt(publicacao.getTimestamp())
                                    + " | relogio=" + publicacao.getRelogio());

                            dados.publicacoes.add(new Dados.PublicacaoSalva(
                                    publicacao.getCanal(), publicacao.getMensagem(),
                                    publicacao.getUsuario(), publicacao.getTimestamp()));
                            salvarDados(dados);
                            resposta.setSucesso(true);
                            resposta.setErro("");
                        }
                        resposta.setTimestamp(agora());
                        resposta.setRelogio(proximoRelogio());

                        System.out.println("[ENVIADO] publicar | sucesso=" + resposta.getSucesso()
                                + " | erro='" + resposta.getErro()
                                + "' | timestamp=" + fmt(resposta.getTimestamp())
                                + " | relogio=" + resposta.getRelogio());

                        ZMsg respostaMsg = new ZMsg();
                        respostaMsg.add("publicar");
                        respostaMsg.add(resposta.build().toByteArray());
                        respostaMsg.send(socket);

                    } catch (InvalidProtocolBufferException e) {
                        System.out.println("erro ao desserializar: " + e.getMessage());
                    }

                } else {
                    System.out.println("tipo de mensagem desconhecido: " + tipo);
                    ZMsg respostaMsg = new ZMsg();
                    respostaMsg.add(tipo);
                    respostaMsg.add(new byte[0]);
                    respostaMsg.send(socket);
                }

                if (mensagensClientes % INTERVALO_HEARTBEAT == 0) {
                    heartbeat();
                    listarServidores();
                }
            }
        }
    }

    private static Dados carregarDados() {
        File arquivo = new File(ARQUIVO_DADOS);
        if (arquivo.exists()) {
            try (FileReader reader = new FileReader(arquivo)) {
                Dados dados = gson.fromJson(reader, Dados.class);
                if (dados != null) {
                    return dados;
                }
            } catch (IOException e) {
                System.out.println("erro ao carregar dados: " + e.getMessage());
            }
        }
        return new Dados();
    }

    private static void salvarDados(Dados dados) {
        try {
            Files.writeString(Path.of(ARQUIVO_DADOS), gson.toJson(dados));
        } catch (IOException e) {
            System.out.println("erro ao salvar dados: " + e.getMessage());
        }
    }
}