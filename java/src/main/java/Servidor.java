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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    static final String TOPICO_REPLICACAO = "replicacao";

    static final int INTERVALO_HEARTBEAT = 10;
    static final int INTERVALO_SINCRONIZACAO = 15;
    static final int INTERVALO_REPLICACAO = 20;
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
    static boolean eleicaoPendente = false;
    static boolean primeiroAnuncio = true;
    static final long TTL_LISTA_MS = 3000;
    static List<Mensagem.ServidorInfo> cacheLista = new ArrayList<>();
    static long cacheHora = 0;

    static ZContext context;
    static ZMQ.Socket ref;
    static ZMQ.Socket pub;
    static ZMQ.Socket peer;
    static ZMQ.Socket sub;
    static Dados dados;
    static final Set<String> chavesLogins = new HashSet<>();
    static final Set<String> chavesPublicacoes = new HashSet<>();

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
        return listarServidores(false);
    }

    static List<Mensagem.ServidorInfo> listarServidores(boolean usarCache) throws InvalidProtocolBufferException {
        if (usarCache && System.currentTimeMillis() - cacheHora < TTL_LISTA_MS) {
            return cacheLista;
        }
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
        cacheLista = resp.getServidoresList();
        cacheHora = System.currentTimeMillis();
        return cacheLista;
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
                + " | timestamp=" + fmt(resp.getTimestamp())
                + " | relogio=" + resp.getRelogio()
                + " | local=" + relogio);
        pausa();
    }

    static byte[] pedirServidor(String nome, String tipo, byte[] payload) {
        ZMQ.Socket s = context.createSocket(SocketType.REQ);
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
            long fim = System.currentTimeMillis() + TIMEOUT_PAR_MS;
            while (System.currentTimeMillis() < fim) {
                // enquanto espera, continua atendendo os outros servers
                if (atenderPar()) {
                    eleicaoPendente = true;
                }
                byte[] tipoResposta = s.recv(ZMQ.DONTWAIT);
                if (tipoResposta != null) {
                    return s.recv();
                }
                Thread.sleep(5);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    static void definirCoordenador(String nome) {
        coordenador = nome;
        System.out.println("[COORDENADOR] coordenador=" + coordenador);
    }

    static void anunciarCoordenador() {
        // espera um pouco por causa do pub/proxy (so no primeiro anuncio quando a conexao e nova agr)
        if (primeiroAnuncio) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            primeiroAnuncio = false;
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
        for (Mensagem.ServidorInfo s : listarServidores(true)) {
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

    static boolean atenderPar() throws InvalidProtocolBufferException {
        byte[] tipoBytes = peer.recv(ZMQ.DONTWAIT);
        if (tipoBytes == null) {
            return false;
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
            return req.getRank() < meuRank; // maior rank assume
        } else if (tipo.equals("dados")) {
            Mensagem.DadosRequest req = Mensagem.DadosRequest.parseFrom(dadosBytes);
            atualizarRelogio(req.getRelogio());
            System.out.println("[RECEBIDO] dados | de=" + req.getNome()
                    + " | timestamp=" + fmt(req.getTimestamp())
                    + " | relogio=" + req.getRelogio()
                    + " | local=" + relogio);
            Mensagem.DadosResponse resp = montarDadosResponse();
            System.out.println("[ENVIADO] dados | para=" + req.getNome()
                    + " | logins=" + resp.getLoginsCount()
                    + " canais=" + resp.getCanaisCount()
                    + " publicacoes=" + resp.getPublicacoesCount()
                    + " | timestamp=" + fmt(resp.getTimestamp())
                    + " | relogio=" + resp.getRelogio());
            ZMsg r = new ZMsg();
            r.add("dados");
            r.add(resp.toByteArray());
            r.send(peer);
        } else {
            System.out.println("tipo de mensagem desconhecido (par): " + tipo);
            ZMsg r = new ZMsg();
            r.add(tipo);
            r.add(new byte[0]);
            r.send(peer);
        }
        return false;
    }

    static String chaveLogin(String usuario, long timestamp) {
        return usuario + "\u0001" + timestamp;
    }

    static String chavePublicacao(String canal, String mensagem, String usuario, long timestamp) {
        return canal + "\u0001" + mensagem + "\u0001" + usuario + "\u0001" + timestamp;
    }

    static void indexarDados() {
        chavesLogins.clear();
        chavesPublicacoes.clear();
        for (Dados.LoginSalvo l : dados.logins) {
            chavesLogins.add(chaveLogin(l.usuario, l.timestamp));
        }
        for (Dados.PublicacaoSalva p : dados.publicacoes) {
            chavesPublicacoes.add(chavePublicacao(p.canal, p.mensagem, p.usuario, p.timestamp));
        }
    }

    // so grava se ainda nao existir nessas 3 funcs!!
    static boolean gravarLogin(String usuario, long timestamp) {
        if (!chavesLogins.add(chaveLogin(usuario, timestamp))) {
            return false;
        }
        dados.logins.add(new Dados.LoginSalvo(usuario, timestamp));
        return true;
    }

    static boolean gravarCanal(String nomeCanal) {
        if (dados.canais.contains(nomeCanal)) {
            return false;
        }
        dados.canais.add(nomeCanal);
        return true;
    }

    static boolean gravarPublicacao(String canal, String mensagem, String usuario, long timestamp, boolean ordenar) {
        if (!chavesPublicacoes.add(chavePublicacao(canal, mensagem, usuario, timestamp))) {
            return false;
        }
        dados.publicacoes.add(new Dados.PublicacaoSalva(canal, mensagem, usuario, timestamp));
        if (ordenar) {
            ordenarPublicacoes();
        }
        return true;
    }

    static void ordenarPublicacoes() {
        dados.publicacoes.sort(Comparator.comparingLong(p -> p.timestamp));
    }

    static void publicarReplicacao(Mensagem.Replicacao.Builder builder, String detalhe) {
        Mensagem.Replicacao rep = builder
                .setOrigem(NOME)
                .setTimestamp(agora())
                .setRelogio(proximoRelogio())
                .build();
        ZMsg msg = new ZMsg();
        msg.add(TOPICO_REPLICACAO);
        msg.add(rep.toByteArray());
        msg.send(pub);
        System.out.println("[REPLICADO] " + detalhe
                + " | timestamp=" + fmt(rep.getTimestamp())
                + " | relogio=" + rep.getRelogio());
    }

    static void replicarLogin(String usuario, long timestamp) {
        publicarReplicacao(Mensagem.Replicacao.newBuilder()
                .setLogin(Mensagem.LoginDado.newBuilder().setUsuario(usuario).setTimestamp(timestamp)),
                "tipo=login | usuario=" + usuario);
    }

    static void replicarCanal(String nomeCanal) {
        publicarReplicacao(Mensagem.Replicacao.newBuilder().setCanal(nomeCanal),
                "tipo=canal | canal=" + nomeCanal);
    }

    static void replicarPublicacao(String canal, String mensagem, String usuario, long timestamp) {
        publicarReplicacao(Mensagem.Replicacao.newBuilder()
                .setPublicacao(Mensagem.PublicacaoDado.newBuilder()
                        .setCanal(canal).setMensagem(mensagem).setUsuario(usuario).setTimestamp(timestamp)),
                "tipo=publicacao | canal=" + canal + " | mensagem='" + mensagem + "'");
    }

    static void aplicarReplicacao(byte[] corpo) throws InvalidProtocolBufferException {
        Mensagem.Replicacao rep = Mensagem.Replicacao.parseFrom(corpo);
        atualizarRelogio(rep.getRelogio());
        if (rep.getOrigem().equals(NOME)) {
            return;
        }
        boolean novo;
        String detalhe;
        switch (rep.getDadoCase()) {
            case LOGIN:
                novo = gravarLogin(rep.getLogin().getUsuario(), rep.getLogin().getTimestamp());
                detalhe = "tipo=login | usuario=" + rep.getLogin().getUsuario();
                break;
            case CANAL:
                novo = gravarCanal(rep.getCanal());
                detalhe = "tipo=canal | canal=" + rep.getCanal();
                break;
            case PUBLICACAO:
                Mensagem.PublicacaoDado p = rep.getPublicacao();
                novo = gravarPublicacao(p.getCanal(), p.getMensagem(), p.getUsuario(), p.getTimestamp(), true);
                detalhe = "tipo=publicacao | canal=" + p.getCanal() + " | mensagem='" + p.getMensagem() + "'";
                break;
            default:
                return;
        }
        if (novo) {
            salvarDados(dados);
        }
        System.out.println("[RECEBIDO] replicacao | origem=" + rep.getOrigem()
                + " | " + detalhe
                + " | novo=" + novo
                + " | timestamp=" + fmt(rep.getTimestamp())
                + " | relogio=" + rep.getRelogio()
                + " | local=" + relogio);
    }

    static Mensagem.DadosResponse montarDadosResponse() {
        Mensagem.DadosResponse.Builder b = Mensagem.DadosResponse.newBuilder();
        for (Dados.LoginSalvo l : dados.logins) {
            b.addLogins(Mensagem.LoginDado.newBuilder().setUsuario(l.usuario).setTimestamp(l.timestamp));
        }
        b.addAllCanais(dados.canais);
        for (Dados.PublicacaoSalva p : dados.publicacoes) {
            b.addPublicacoes(Mensagem.PublicacaoDado.newBuilder()
                    .setCanal(p.canal).setMensagem(p.mensagem).setUsuario(p.usuario).setTimestamp(p.timestamp));
        }
        b.setTimestamp(agora());
        b.setRelogio(proximoRelogio());
        return b.build();
    }

    static void sincronizarDados() throws InvalidProtocolBufferException {
        for (Mensagem.ServidorInfo s : listarServidores(true)) {
            if (s.getNome().equals(NOME)) {
                continue;
            }
            Mensagem.DadosRequest req = Mensagem.DadosRequest.newBuilder()
                    .setNome(NOME)
                    .setTimestamp(agora())
                    .setRelogio(proximoRelogio())
                    .build();
            System.out.println("[ENVIADO] dados | para=" + s.getNome()
                    + " | timestamp=" + fmt(req.getTimestamp())
                    + " | relogio=" + req.getRelogio());
            byte[] resposta = pedirServidor(s.getNome(), "dados", req.toByteArray());
            if (resposta == null) {
                System.out.println("[REPLICACAO] " + s.getNome() + " nao respondeu ao pedido de dados");
                continue;
            }
            Mensagem.DadosResponse resp = Mensagem.DadosResponse.parseFrom(resposta);
            atualizarRelogio(resp.getRelogio());
            int novosLogins = 0;
            int novosCanais = 0;
            int novasPublicacoes = 0;
            for (Mensagem.LoginDado l : resp.getLoginsList()) {
                if (gravarLogin(l.getUsuario(), l.getTimestamp())) {
                    novosLogins++;
                }
            }
            for (String c : resp.getCanaisList()) {
                if (gravarCanal(c)) {
                    novosCanais++;
                }
            }
            for (Mensagem.PublicacaoDado p : resp.getPublicacoesList()) {
                if (gravarPublicacao(p.getCanal(), p.getMensagem(), p.getUsuario(), p.getTimestamp(), false)) {
                    novasPublicacoes++;
                }
            }
            if (novosLogins > 0 || novosCanais > 0 || novasPublicacoes > 0) {
                ordenarPublicacoes();
                salvarDados(dados);
            }
            System.out.println("[RECEBIDO] dados | de=" + s.getNome()
                    + " | recebidos: logins=" + resp.getLoginsCount()
                    + " canais=" + resp.getCanaisCount()
                    + " publicacoes=" + resp.getPublicacoesCount()
                    + " | novos aqui: logins=" + novosLogins
                    + " canais=" + novosCanais
                    + " publicacoes=" + novasPublicacoes
                    + " | timestamp=" + fmt(resp.getTimestamp())
                    + " | relogio=" + resp.getRelogio()
                    + " | local=" + relogio);
        }
    }

    static boolean atenderAnuncio() throws InvalidProtocolBufferException {
        byte[] topico = sub.recv(ZMQ.DONTWAIT);
        if (topico == null) {
            return false;
        }
        byte[] corpo = sub.recv();
        if (new String(topico).equals(TOPICO_REPLICACAO)) {
            aplicarReplicacao(corpo);
            return true;
        }
        Mensagem.CoordenadorAnuncio anuncio = Mensagem.CoordenadorAnuncio.parseFrom(corpo);
        atualizarRelogio(anuncio.getRelogio());
        System.out.println("[RECEBIDO] topico=" + TOPICO_SERVERS
                + " | coordenador=" + anuncio.getNome()
                + " | timestamp=" + fmt(anuncio.getTimestamp())
                + " | relogio=" + anuncio.getRelogio()
                + " | local=" + relogio);
        boolean eraCoordenador = NOME.equals(coordenador);
        definirCoordenador(anuncio.getNome());
        if (!anuncio.getNome().equals(NOME)) {
            for (Mensagem.ServidorInfo si : listarServidores(true)) {
                if (si.getNome().equals(anuncio.getNome()) && si.getRank() < meuRank) {
                    if (eraCoordenador) {
                        System.out.println("[ELEICAO] " + anuncio.getNome() + " tem rank menor que " + NOME + ", reafirmando coordenacao");
                        definirCoordenador(NOME);
                        anunciarCoordenador();
                    } else {
                        System.out.println("[ELEICAO] " + anuncio.getNome() + " tem rank menor que " + NOME + ", nova eleicao");
                        iniciarEleicao();
                    }
                    break;
                }
            }
        }
        return true;
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
            sub.subscribe(TOPICO_REPLICACAO.getBytes());

            socket.setReceiveTimeOut(200);

            System.out.println("servidor java conectado ao broker em " + broker + ", ao proxy em " + proxyPub
                    + "/" + PROXY_SUB + " e ouvindo servidores em " + PEER_BIND);

            dados = carregarDados();
            indexarDados();

            iniciarEleicao();
            sincronizarDados();

            while (!Thread.currentThread().isInterrupted()) {
                while (atenderAnuncio()) {
                    // le tudo q chegou no sub
                }
                if (atenderPar()) {
                    eleicaoPendente = true;
                }
                if (eleicaoPendente) {
                    iniciarEleicao();
                    eleicaoPendente = false;
                }

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
                            gravarLogin(loginReq.getUsuario(), loginReq.getTimestamp());
                            salvarDados(dados);
                            replicarLogin(loginReq.getUsuario(), loginReq.getTimestamp());
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
                            gravarCanal(canalReq.getNomeCanal());
                            salvarDados(dados);
                            replicarCanal(canalReq.getNomeCanal());
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

                            gravarPublicacao(publicacao.getCanal(), publicacao.getMensagem(),
                                    publicacao.getUsuario(), publicacao.getTimestamp(), true);
                            salvarDados(dados);
                            replicarPublicacao(publicacao.getCanal(), publicacao.getMensagem(),
                                    publicacao.getUsuario(), publicacao.getTimestamp());
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

                if (mensagensClientes % INTERVALO_REPLICACAO == 0) {
                    sincronizarDados();
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