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
    static final String PROXY_SUB = System.getenv().getOrDefault("PROXY_SUB_ENDERECO", "tcp://proxy:5558");
    static final String PEER_PORTA = System.getenv().getOrDefault("PEER_PORTA", "5600");
    static final String PEER_BIND = System.getenv().getOrDefault("PEER_BIND", "tcp://*:" + PEER_PORTA);
    static final String TOPICO_SERVERS = "servers";

    static final int INTERVALO_HEARTBEAT = 10;
    static final int INTERVALO_SINCRONIZACAO = 15;
    static final int TIMEOUT_MS = 3000;
    static final int TIMEOUT_PAR_MS = 2000;
    static final int TENTATIVAS = 20;

    static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    static long relogio = 0;
    static long desvio = Long.parseLong(System.getenv().getOrDefault("DESVIO_RELOGIO_MS", "0"));
    static int mensagensClientes = 0;
    static int meuRank = 0;
    static String coordenador = null; // nome do serv q sera coodenador

    static ZContext context;
    static ZMQ.Socket ref;
    static ZMQ.Socket pub;
    static ZMQ.Socket peer;
    static ZMQ.Socket sub;

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

    static List<Mensagem.ServidorInfo> listarServidores() throws InvalidProtocolBufferException {
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
        return resp.getServidoresList();
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

        byte[] dados = pedirReferencia("heartbeat", req.toByteArray());

        Mensagem.HeartbeatResponse resp = Mensagem.HeartbeatResponse.parseFrom(dados);
        atualizarRelogio(resp.getRelogio());
        System.out.println("[RECEBIDO] heartbeat | ok=" + resp.getOk()
                + " | relogio=" + resp.getRelogio()
                + " | local=" + relogio);
        pausa();
    }

    static byte[] pedirServidor(String nome, String tipo, byte[] payload) {
        ZMQ.Socket s = context.createSocket(SocketType.REQ);
        s.setReceiveTimeOut(TIMEOUT_PAR_MS);
        s.setSendTimeOut(TIMEOUT_PAR_MS);
        s.setLinger(0);
        try {
            s.connect("tcp://" + nome + ":" + PEER_PORTA);
            ZMsg msg = new ZMsg();
            msg.add(tipo);
            msg.add(payload);
            if (!msg.send(s)) {
                return null;
            }
            ZMsg resposta = ZMsg.recvMsg(s);
            if (resposta == null) {
                return null;
            }
            resposta.popString();
            return resposta.pop().getData();
        } catch (Exception e) {
            return null;
        } finally {
            context.destroySocket(s);
        }
    }

    static void definirCoordenador(String nome) {
        coordenador = nome;
        System.out.println("[COORDENADOR] coordenador=" + coordenador);
    }

    static void anunciarCoordenador() {
        // espera um pouco por causa do pub/proxy
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Mensagem.CoordenadorAnuncio anuncio = Mensagem.CoordenadorAnuncio.newBuilder()
                .setNome(NOME)
                .setTimestamp(agora())
                .setRelogio(proximoRelogio())
                .build();
        ZMsg msg = new ZMsg();
        msg.add(TOPICO_SERVERS);
        msg.add(anuncio.toByteArray());
        msg.send(pub);
        System.out.println("[PUBLICADO] topico=" + TOPICO_SERVERS
                + " | coordenador=" + anuncio.getNome()
                + " | timestamp=" + fmt(anuncio.getTimestamp())
                + " | relogio=" + anuncio.getRelogio());
    }

    static void iniciarEleicao() throws InvalidProtocolBufferException {
        coordenador = null;
        System.out.println("[ELEICAO] iniciada por " + NOME + " (rank=" + meuRank + ")");
        List<Mensagem.ServidorInfo> superiores = new ArrayList<>();
        for (Mensagem.ServidorInfo s : listarServidores()) {
            if (s.getRank() > meuRank) {
                superiores.add(s);
            }
        }
        superiores.sort((a, b) -> Integer.compare(a.getRank(), b.getRank()));

        boolean alguemRespondeu = false;
        for (Mensagem.ServidorInfo s : superiores) {
            Mensagem.EleicaoRequest req = Mensagem.EleicaoRequest.newBuilder()
                    .setNome(NOME)
                    .setRank(meuRank)
                    .setTimestamp(agora())
                    .setRelogio(proximoRelogio())
                    .build();
            System.out.println("[ENVIADO] eleicao | para=" + s.getNome()
                    + " | rank=" + req.getRank()
                    + " | timestamp=" + fmt(req.getTimestamp())
                    + " | relogio=" + req.getRelogio());
            byte[] dados = pedirServidor(s.getNome(), "eleicao", req.toByteArray());
            if (dados == null) {
                System.out.println("[ELEICAO] " + s.getNome() + " nao respondeu");
                continue;
            }
            Mensagem.EleicaoResponse resp = Mensagem.EleicaoResponse.parseFrom(dados);
            atualizarRelogio(resp.getRelogio());
            System.out.println("[RECEBIDO] eleicao | de=" + s.getNome()
                    + " | resposta=" + resp.getResposta()
                    + " | timestamp=" + fmt(resp.getTimestamp())
                    + " | relogio=" + resp.getRelogio()
                    + " | local=" + relogio);
            alguemRespondeu = true;
        }

        if (alguemRespondeu) {
            System.out.println("[ELEICAO] " + NOME + " aguarda o anuncio do coordenador");
            return;
        }

        definirCoordenador(NOME);
        anunciarCoordenador();
    }

    static void sincronizarRelogio() throws InvalidProtocolBufferException {
        if (coordenador == null) {
            iniciarEleicao();
            return;
        }
        if (coordenador.equals(NOME)) {
            System.out.println("[SINCRONIZACAO] " + NOME + " e o coordenador, mantem o proprio relogio");
            return;
        }

        Mensagem.RelogioRequest req = Mensagem.RelogioRequest.newBuilder()
                .setNome(NOME)
                .setTimestamp(agora())
                .setRelogio(proximoRelogio())
                .build();
        System.out.println("[ENVIADO] relogio | para=" + coordenador
                + " | timestamp=" + fmt(req.getTimestamp())
                + " | relogio=" + req.getRelogio());

        long t0 = agoraSistema();
        byte[] dados = pedirServidor(coordenador, "relogio", req.toByteArray());
        long t1 = agoraSistema();

        if (dados == null) {
            System.out.println("[COORDENADOR] " + coordenador + " indisponivel");
            iniciarEleicao();
            return;
        }

        Mensagem.RelogioResponse resp = Mensagem.RelogioResponse.parseFrom(dados);
        atualizarRelogio(resp.getRelogio());
        System.out.println("[RECEBIDO] relogio | de=" + coordenador
                + " | timestamp=" + fmt(resp.getTimestamp())
                + " | relogio=" + resp.getRelogio()
                + " | local=" + relogio);

        long estimada = resp.getTimestamp() + (t1 - t0) / 2;
        long novoDesvio = estimada - t1;
        long ajuste = novoDesvio - desvio;
        desvio = novoDesvio;
        System.out.println("[SINCRONIZACAO] coordenador=" + coordenador
                + " | hora=" + fmt(resp.getTimestamp())
                + " | rtt=" + (t1 - t0) + "ms"
                + " | ajuste=" + String.format("%+d", ajuste) + "ms"
                + " | desvio=" + String.format("%+d", desvio) + "ms");
        pausa();
    }

    static void atenderPar() throws InvalidProtocolBufferException {
        byte[] tipoBytes = peer.recv(ZMQ.DONTWAIT);
        if (tipoBytes == null) {
            return;
        }
        String tipo = new String(tipoBytes);
        byte[] dadosBytes = peer.recv();

        if (tipo.equals("relogio")) {
            Mensagem.RelogioRequest req = Mensagem.RelogioRequest.parseFrom(dadosBytes);
            atualizarRelogio(req.getRelogio());
            System.out.println("[RECEBIDO] relogio | de=" + req.getNome()
                    + " | timestamp=" + fmt(req.getTimestamp())
                    + " | relogio=" + req.getRelogio()
                    + " | local=" + relogio);
            Mensagem.RelogioResponse resp = Mensagem.RelogioResponse.newBuilder()
                    .setTimestamp(agora())
                    .setRelogio(proximoRelogio())
                    .build();
            System.out.println("[ENVIADO] relogio | para=" + req.getNome()
                    + " | timestamp=" + fmt(resp.getTimestamp())
                    + " | relogio=" + resp.getRelogio());
            ZMsg r = new ZMsg();
            r.add("relogio");
            r.add(resp.toByteArray());
            r.send(peer);
        } else if (tipo.equals("eleicao")) {
            Mensagem.EleicaoRequest req = Mensagem.EleicaoRequest.parseFrom(dadosBytes);
            atualizarRelogio(req.getRelogio());
            System.out.println("[RECEBIDO] eleicao | de=" + req.getNome()
                    + " | rank=" + req.getRank()
                    + " | timestamp=" + fmt(req.getTimestamp())
                    + " | relogio=" + req.getRelogio()
                    + " | local=" + relogio);
            Mensagem.EleicaoResponse resp = Mensagem.EleicaoResponse.newBuilder()
                    .setResposta("OK")
                    .setTimestamp(agora())
                    .setRelogio(proximoRelogio())
                    .build();
            System.out.println("[ENVIADO] eleicao | para=" + req.getNome()
                    + " | resposta=" + resp.getResposta()
                    + " | timestamp=" + fmt(resp.getTimestamp())
                    + " | relogio=" + resp.getRelogio());
            ZMsg r = new ZMsg();
            r.add("eleicao");
            r.add(resp.toByteArray());
            r.send(peer);
            if (req.getRank() < meuRank) {
                iniciarEleicao(); // maior rank assume
            }
        } else {
            System.out.println("tipo de mensagem desconhecido (par): " + tipo);
            ZMsg r = new ZMsg();
            r.add(tipo);
            r.add(new byte[0]);
            r.send(peer);
        }
    }

    static void atenderAnuncio() throws InvalidProtocolBufferException {
        byte[] topico = sub.recv(ZMQ.DONTWAIT);
        if (topico == null) {
            return;
        }
        Mensagem.CoordenadorAnuncio anuncio = Mensagem.CoordenadorAnuncio.parseFrom(sub.recv());
        atualizarRelogio(anuncio.getRelogio());
        System.out.println("[RECEBIDO] topico=" + TOPICO_SERVERS
                + " | coordenador=" + anuncio.getNome()
                + " | timestamp=" + fmt(anuncio.getTimestamp())
                + " | relogio=" + anuncio.getRelogio()
                + " | local=" + relogio);
        definirCoordenador(anuncio.getNome());
        if (!anuncio.getNome().equals(NOME)) {
            for (Mensagem.ServidorInfo si : listarServidores()) {
                if (si.getNome().equals(anuncio.getNome()) && si.getRank() < meuRank) {
                    System.out.println("[ELEICAO] " + anuncio.getNome() + " tem rank menor que " + NOME + ", nova eleicao");
                    iniciarEleicao();
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try (ZContext ctx = new ZContext()) {
            context = ctx;
            ref = novoSocketReferencia();

            int rank = obterRank();
            meuRank = rank;
            System.out.println("servidor " + NOME + " cadastrado com rank " + rank);
            listarServidores();

            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            String broker = System.getenv().getOrDefault("BROKER_ENDERECO", "tcp://localhost:5556");
            socket.connect(broker);

            pub = context.createSocket(SocketType.PUB);
            String proxyPub = System.getenv().getOrDefault("PROXY_PUB_ENDERECO", "tcp://localhost:5557");
            pub.connect(proxyPub);

            peer = context.createSocket(SocketType.REP);
            peer.bind(PEER_BIND);

            sub = context.createSocket(SocketType.SUB);
            sub.connect(PROXY_SUB);
            sub.subscribe(TOPICO_SERVERS.getBytes());

            socket.setReceiveTimeOut(200);

            System.out.println("servidor java conectado ao broker em " + broker + ", ao proxy em " + proxyPub
                    + "/" + PROXY_SUB + " e ouvindo servidores em " + PEER_BIND);

            Dados dados = carregarDados();

            iniciarEleicao();

            while (!Thread.currentThread().isInterrupted()) {
                atenderAnuncio();
                atenderPar();

                ZMsg msg = ZMsg.recvMsg(socket);
                if (msg == null) {
                    continue;
                }
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

                if (mensagensClientes % INTERVALO_SINCRONIZACAO == 0) {
                    sincronizarRelogio();
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