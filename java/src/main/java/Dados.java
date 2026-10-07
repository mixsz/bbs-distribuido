import java.util.ArrayList;
import java.util.List;

public class Dados {
    public List<LoginSalvo> logins = new ArrayList<>();
    public List<String> canais = new ArrayList<>();

    public static class LoginSalvo {
        public String usuario;
        public long timestamp;

        public LoginSalvo(String usuario, long timestamp) {
            this.usuario = usuario;
            this.timestamp = timestamp;
        }
    }
}