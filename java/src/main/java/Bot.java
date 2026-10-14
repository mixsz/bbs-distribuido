import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Bot {
    static final String ENDERECO = System.getenv().getOrDefault("SERVIDOR_ENDERECO", "tcp://localhost:5555");
    static final String PROXY_SUB = System.getenv().getOrDefault("PROXY_SUB_ENDERECO", "tcp://localhost:5558");
    static final String USUARIO = System.getenv().getOrDefault("USUARIO", "bot-java");

    static final int TIMEOUT_MS = 3000;
    static final int TENTATIVAS = 10;
    static final int MIN_CANAIS = 5;
    static final int MIN_INSCRICOES = 3;
    static final int MENSAGENS_POR_RODADA = 10;

    static final String[] FRASES = {
            "alo",
            "haha",
            "teste",
            "mais uma mensagem",
            "agua",
            "papepipopu",
            "hey",
            "fadsjpoifjaspfa"
    };

    static final DateTimeFormatter FMT_MS =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    static ZContext context = new ZContext();
    static ZMQ.Socket socket = novoSocket();
    static ZMQ.Socket sub = novoSub();
    static Set<String> inscritos = new HashSet<>();
    static Random random = new Random();

    static ZMQ.Socket novoSocket() {
        ZMQ.Socket s = context.createSocket(SocketType.REQ);
        s.setReceiveTimeOut(TIMEOUT_MS);
        s.setLinger(0);
        s.connect(ENDERECO);
        return s;
    }

    static ZMQ.Socket novoSub() {
        ZMQ.Socket s = context.createSocket(SocketType.SUB);
        s.connect(PROXY_SUB);
        return s;
    }

    static String fmt(long ts) {
        return FMT_MS.format(Instant.ofEpochMilli(ts));
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

    static void esperar(long milissegundos) throws Exception {
        long fim = System.currentTimeMillis() + milissegundos;
        while (true) {
            long restante = fim - System.currentTimeMillis();
            if (restante <= 0) {
                break;
            }
            sub.setReceiveTimeOut((int) Math.max(1, restante));
            ZMsg msg = ZMsg.recvMsg(sub);
            if (msg == null) {
                continue;
            }
            String topico = msg.popString();
            byte[] dados = msg.pop().getData();
            if (!inscritos.contains(topico)) {
                continue;
            }
            Mensagem.Publicacao publicacao = Mensagem.Publicacao.parseFrom(dados);
            System.out.println("[PUBLICACAO] canal=" + publicacao.getCanal()
                    + " | mensagem='" + publicacao.getMensagem()
                    + "' | usuario=" + publicacao.getUsuario()
                    + " | enviada=" + fmt(publicacao.getTimestamp())
                    + " | recebida=" + fmt(System.currentTimeMillis()));
        }
    }

    static void login() throws Exception {
        Mensagem.LoginRequest req = Mensagem.LoginRequest.newBuilder()
                .setUsuario(USUARIO)
                .setTimestamp(System.currentTimeMillis())
                .build();
        System.out.println("[ENVIADO] login | usuario=" + req.getUsuario()
                + " | timestamp=" + fmt(req.getTimestamp()));
        Mensagem.LoginResponse resp =
                Mensagem.LoginResponse.parseFrom(enviar("login", req.toByteArray()));
        System.out.println("[RECEBIDO] login | sucesso=" + resp.getSucesso()
                + " | erro='" + resp.getErro()
                + "' | timestamp=" + fmt(resp.getTimestamp()));
    }

    static List<String> listarCanais() throws Exception {
        Mensagem.ListarCanaisRequest req = Mensagem.ListarCanaisRequest.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .build();
        System.out.println("[ENVIADO] listar_canais | timestamp=" + fmt(req.getTimestamp()));
        Mensagem.ListarCanaisResponse resp =
                Mensagem.ListarCanaisResponse.parseFrom(enviar("listar_canais", req.toByteArray()));
        System.out.println("[RECEBIDO] listar_canais | canais=" + resp.getCanaisList()
                + " | timestamp=" + fmt(resp.getTimestamp()));
        return new ArrayList<>(resp.getCanaisList());
    }

    static boolean criarCanal(String nome) throws Exception {
        Mensagem.CriarCanalRequest req = Mensagem.CriarCanalRequest.newBuilder()
                .setNomeCanal(nome)
                .setTimestamp(System.currentTimeMillis())
                .build();
        System.out.println("[ENVIADO] criar_canal | nome_canal=" + req.getNomeCanal()
                + " | timestamp=" + fmt(req.getTimestamp()));
        Mensagem.CriarCanalResponse resp =
                Mensagem.CriarCanalResponse.parseFrom(enviar("criar_canal", req.toByteArray()));
        System.out.println("[RECEBIDO] criar_canal | sucesso=" + resp.getSucesso()
                + " | erro='" + resp.getErro()
                + "' | timestamp=" + fmt(resp.getTimestamp()));
        return resp.getSucesso();
    }

    static void publicar(String canal, String mensagem) throws Exception {
        Mensagem.PublicarRequest req = Mensagem.PublicarRequest.newBuilder()
                .setUsuario(USUARIO)
                .setCanal(canal)
                .setMensagem(mensagem)
                .setTimestamp(System.currentTimeMillis())
                .build();
        System.out.println("[ENVIADO] publicar | canal=" + req.getCanal()
                + " | mensagem='" + req.getMensagem()
                + "' | timestamp=" + fmt(req.getTimestamp()));
        Mensagem.PublicarResponse resp =
                Mensagem.PublicarResponse.parseFrom(enviar("publicar", req.toByteArray()));
        System.out.println("[RECEBIDO] publicar | sucesso=" + resp.getSucesso()
                + " | erro='" + resp.getErro()
                + "' | timestamp=" + fmt(resp.getTimestamp()));
    }

    public static void main(String[] args) throws Exception {
        login();
        esperar(1000);

        while (true) {
            esperar((long) (random.nextDouble() * 1500));
            List<String> canais = listarCanais();

            if (canais.size() < MIN_CANAIS) {
                String nome = "canal-" + (1000 + random.nextInt(9000));
                if (criarCanal(nome)) {
                    canais.add(nome);
                }
            }

            if (inscritos.size() < MIN_INSCRICOES) {
                List<String> disponiveis = new ArrayList<>();
                for (String c : canais) {
                    if (!inscritos.contains(c)) {
                        disponiveis.add(c);
                    }
                }
                if (!disponiveis.isEmpty()) {
                    String escolhido = disponiveis.get(random.nextInt(disponiveis.size()));
                    sub.subscribe(escolhido);
                    inscritos.add(escolhido);
                    System.out.println("[INSCRICAO] canal=" + escolhido + " | total=" + inscritos.size());
                }
            }

            if (canais.isEmpty()) {
                esperar(1000);
                continue;
            }

            String canal = canais.get(random.nextInt(canais.size()));
            for (int i = 1; i <= MENSAGENS_POR_RODADA; i++) {
                publicar(canal, FRASES[random.nextInt(FRASES.length)] + " #" + i);
                esperar(1000);
            }
        }
    }
}