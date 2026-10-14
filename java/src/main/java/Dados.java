import java.util.ArrayList;
import java.util.List;

public class Dados {
    public List<LoginSalvo> logins = new ArrayList<>();
    public List<String> canais = new ArrayList<>();
    public List<PublicacaoSalva> publicacoes = new ArrayList<>();

    public static class LoginSalvo {
        public String usuario;
        public long timestamp;

        public LoginSalvo(String usuario, long timestamp) {
            this.usuario = usuario;
            this.timestamp = timestamp;
        }
    }

    public static class PublicacaoSalva {
        public String canal;
        public String mensagem;
        public String usuario;
        public long timestamp;

        public PublicacaoSalva(String canal, String mensagem, String usuario, long timestamp) {
            this.canal = canal;
            this.mensagem = mensagem;
            this.usuario = usuario;
            this.timestamp = timestamp;
        }
    }
}