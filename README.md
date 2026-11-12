# bbs-distribuido

## Integrantes

- Danilo Cardoso Pradella
- Caue Yusuke 

Sistema de troca de mensagens instantâneas no estilo BBS/IRC, feito para a disciplina de Sistemas Paralelos e Distribuídos. Bots criam canais, publicam mensagens e assinam canais, e vários servidores atendem os bots. O projeto foi feito com duas linguagens (Python e Java), com servidor e bot nas duas, e os componentes de uma linguagem conversam normalmente com os da outra.

## Como rodar

```bash
docker compose up --build
```

Tudo sobe pelo orquestrador, sem interação do usuário. O compose executa 4 servidores (2 em py e 2 em java) e 4 bots (2 em py e 2 em java). Os dados de cada servidor ficam em `./dados/python`, `./dados/python-2`, `./dados/java` e `./dados/java-2` (pasta fora do git).

## Como acompanhar a execução

Com o sistema rodando, em outro terminal. Os comandos filtram só os eventos principais, para facilitar a leitura (os bots geram muito log e se aumentar a PAUSA_MS (compose) pelo menos um servidor não consegue se conectar).

**Eleição, coordenador e sincronização de relógio** (nos 4 servidores):

```bash
docker compose logs -f servidor-python servidor-python-2 servidor-java servidor-java-2 | grep --line-buffered -E "ELEICAO|COORDENADOR\]|SINCRONIZACAO"
```

**Registro, remoção e reativação dos servidores** (na referência):

```bash
docker compose logs -f referencia | grep --line-buffered -E "REGISTRADO|REMOVIDO|REATIVADO"
```

Para testar a reeleição, derrube o coordenador com `docker stop servidor-x` (troca x pelo de maior rank). Depois de alguns segundos os outros servidores abrem uma nova eleição e o servidor de maior rank que sobrou vira coordenador. Após 30 segundos sem heartbeat a referência o marca como `REMOVIDO`. Ao subir de novo com `docker start servidor-x` (troca x pelo que foi derrubado), ele aparece como `REATIVADO`, mantém o rank e reassume a coordenação. Depois do `docker start`, reinicie o comando de logs, pois ele não reanexa ao container reiniciado.

## Componentes

| Serviço | Linguagem | Função |
| --- | --- | --- |
| `broker` | Python | Proxy ROUTER/DEALER do Req/Rep: bots na porta 5555, servidores na 5556, com balanceamento round-robin |
| `proxy` | Python | Proxy XSUB/XPUB do Pub/Sub: servidores publicam na 5557, bots e servidores assinam na 5558 |
| `referencia` | Python | Dá o rank de cada servidor, guarda a lista de servidores ativos e remove quem para de enviar heartbeat |
| `servidor-python`, `servidor-python-2` | Python | Atendem os bots, publicam nos canais, guardam os dados em disco e replicam entre si |
| `servidor-java`, `servidor-java-2` | Java | Mesma coisa que os servidores Python |
| `bot-python`, `bot-python-2` | Python | Clientes automáticos: login, criar canal, listar canais, assinar e publicar |
| `bot-java`, `bot-java-2` | Java | Igual aos bots Python |

## Escolhas de projeto

- **Linguagens:** Python e Java. Cada uma tem servidor e bot, e eles se comunicam entre si. Broker, proxy e referência estão implementados em Python.
- **Troca de mensagens:** ZeroMQ. Req/Rep entre bots e servidores (passando pelo `broker`), Pub/Sub para publicações de canais (passando pelo `proxy`) e Req/Rep direto entre servidores (porta 5600 de cada um) para relógio, eleição e cópia de dados.
- **Serialização:** Protobuf (`proto/mensagem.proto`), formato binário. Cada mensagem Req/Rep vai em duas partes (multipart): o tipo (por exemplo `login`) e os bytes do Protobuf. Toda mensagem leva o timestamp do envio e o relógio lógico de quem envia, inclusive a resposta do heartbeat.
- **Formato das mensagens:** uma mensagem Protobuf por operação (`LoginRequest`/`LoginResponse`, `CriarCanalRequest`/`CriarCanalResponse`, `PublicarRequest`/`PublicarResponse`, `RankRequest`/`RankResponse`, `HeartbeatRequest`/`HeartbeatResponse`, e assim por diante). As respostas de erro trazem `sucesso=false` e um texto em `erro`. Quando o login falha, o bot repete o login.
- **Persistência:** cada servidor tem o próprio `dados.json` com logins (com o timestamp do login), canais e publicações, carregado ao iniciar e salvo a cada mudança.
- **Relógio lógico:** contador de Lamport em bots, servidores e referência (incrementa ao enviar, vira o máximo ao receber).
- **Referência e heartbeat:** o servidor pede o rank à referência ao subir e envia heartbeat a cada 10 mensagens de clientes. Quem fica mais de 30 segundos sem enviar é removido da lista. A resposta do heartbeat não traz mais a hora para acertar o relógio, só confirma que o servidor está na lista.
- **Eleição:** o coordenador é o servidor ativo de maior rank, escolhido pelo algoritmo do valentão (Bully). Um servidor que percebe que o coordenador não responde pede eleição aos servidores de rank maior (`REQ: eleição`, `REP: OK`). Se ninguém de rank maior responde, ele se declara coordenador e publica o próprio nome no tópico `servers`. Cada servidor guarda numa variável o nome do coordenador. Quem recebe o anúncio de um servidor de rank menor que o seu abre uma nova eleição (ou, se já era o coordenador, apenas reafirma o anúncio).
- **Relógio físico:** Berkeley como visto em aula. A cada 15 mensagens de clientes, cada servidor pede a hora ao coordenador (`REQ: relógio`, `REP: hora correta`), soma metade do RTT a hora recebida e ajusta o próprio relógio. O coordenador é a referência. Os servidores Java têm desvio simulado de 4s para a sincronização aparecer nos testes.

## Replicação dos dados (parte 5)

**Problema:** o `broker` distribui as requisições em round-robin, então cada servidor só vê parte das mensagens. Se um cai, perde-se parte do histórico. O objetivo é que todos tenham todos os dados (logins, canais e publicações).

**Método:** replicação ativa (multi-master) com consistência eventual. Qualquer servidor aceita escritas e propaga cada gravação para os outros, que aplicam a mesma. Escolhemos esse método porque o `broker` já espalha as escritas, então não há um servidor único que receba tudo; uma réplica primária exigiria mudar o `broker`.

**Como foi implementado:**

1. **Propagação por Pub/Sub:** após gravar um login, canal ou publicação, o servidor publica uma `Replicacao` no tópico `replicacao`, pelo mesmo `proxy`. Todos assinam e aplicam o que chega, ignorando as próprias mensagens.
2. **Sem duplicar:** cada dado tem uma chave de conteúdo (login: usuário e timestamp; publicação: canal, mensagem, usuário e timestamp; canal: nome). Só entra se a chave ainda não existe, então receber o mesmo dado duas vezes não duplica nada.
3. **Cópia completa por Req/Rep:** o servidor pede todos os dados a cada um dos outros (`DadosRequest`/`DadosResponse`, porta 5600) ao subir e a cada 20 mensagens de clientes.
4. **Ordem:** as publicações ficam ordenadas pelo timestamp, e como os relógios são sincronizados pelo coordenador, a ordem é igual em todos.
5. **Relógio lógico:** as mensagens de replicação e de cópia carregam e atualizam o relógio lógico.

**Mudanças feitas no método:**

- **Pub/Sub sozinho perde mensagens** (não guarda para quem está desconectado). Por isso foi acrescentada a cópia completa por Req/Rep, que faz um servidor que caiu e voltou recuperar tudo.
- **Sem ordem total entre escritas:** os servidores não coordenam a ordem, então o sistema é eventualmente consistente. Como as operações só adicionam dados e a chave de conteúdo é idempotente, a ordem de chegada não altera o resultado.
- **O coordenador não é réplica primária:** a eleição continua servindo só ao relógio.
- **Servidores atendem os pares enquanto esperam resposta:** com 4 servidores, quem esperava deixava de atender os demais e os timeouts os davam como mortos. A lista de servidores da referência também passou a ser guardada por 3 segundos.

**Limitações:** há uma janela curta entre a gravação e a chegada nos outros em que os dados diferem; a cópia completa cresce com o histórico; dois canais criados com o mesmo nome ao mesmo tempo viram um só após a junção; e o `PAUSA_MS` não deve passar de cerca de 400 ms com 4 servidores e 4 bots.
