# Relatório Meta 1

Dinis Maia - 2024237534 ; João Leitão - 2016227984

## 1. Arquitetura do software

O Googol é composto por quatro programas que comunicam por gRPC. Os serviços Gateway e Barrel estão definidos em `googol/googol.proto`. O funcionamento geral é o seguinte:

- O Client só comunica com a Gateway: envia URLs para indexar, pesquisas e subscreve estatísticas;
- A Gateway guarda a fila de URLs. Os Downloaders pedem-lhe URLs (`takeNext`), devolvem os links encontrados (`putMany`) e confirmam quando terminam (`done`);
- Cada Downloader analisa a página e envia o resultado a todos os Barrels (`addToIndex`), através de um protocolo de multicast fiável;
- Os Barrels são réplicas que se registam na Gateway (`registerBarrel`);
- Quando o Client faz uma pesquisa, a Gateway escolhe um Barrel ativo e reencaminha-lhe o pedido. Caso esse Barrel falhe, tenta outro.

| Programa | Ficheiros | Função |
| --- | --- | --- |
| Gateway | `gateway.py`, `url_queue.py` | Comunica com os Barrels e guarda a fila de URLs, as contagens de pesquisas e a lista de Barrels; não guarda o índice |
| Barrel | `barrel.py`, `barrel_store.py` | Armazena o índice (palavras e páginas que as contêm), persistido em SQLite |
| Downloader | `downloader.py`, `parser.py`, `multicast.py` | Obtém e analisa as páginas web (requests e BeautifulSoup) e envia-as aos Barrels |
| Client | `client.py` | Interface para o utilizador |

`url_queue.py` implementa a fila usada na Gateway, `barrel_store.py` o armazenamento do Barrel e `multicast.py` o multicast fiável. Toda a configuração (IPs, portas, retries, endereços dos Barrels) está em `config.properties`.

**Estrutura do índice** (`barrel_store.py`): `words(word, url)` é o índice invertido; `pages(url, title, citation)` guarda a informação de cada página; `links(src, dst)` guarda as ligações, e os backlinks de uma página são as origens que apontam para ela (critério de ordenação dos resultados); `messages(msg_id, payload)` guarda as mensagens já aplicadas. O tamanho do índice descrito nas estatísticas é o número de palavras distintas.

## 2. Replicação do índice (reliable multicast)

Multicast fiável iniciado pelo emissor (o Downloader), com ACK positivo, retransmissão e recetores idempotentes:

1. Cada mensagem recebe um `msg_id` único (`<downloader>:<seq>`) e é gravada num log em disco (*outbox*, com `fsync`) antes de ser enviada.
2. Existe uma thread por Barrel. Cada uma invoca `addToIndex` até obter ACK, com *backoff* exponencial (0,5 s até 10 s) e *timeout* de 5 s por chamada. Um Barrel avariado não atrasa o outro.
3. O ACK é o regresso da chamada sem erro e fica registado no log. Se o Downloader reiniciar com o mesmo `--id`, retoma só o que falta e continua a numeração.
4. O Barrel só responde depois de gravar a mensagem numa transação SQLite (`synchronous=FULL`) e ignora `msg_id` já aplicados. Retransmitir é, por isso, seguro: entrega *pelo menos uma vez* mais deduplicação.

Não é necessária ordenação total: acrescentar a conjuntos é comutativo e idempotente (o título e a citação seguem a última escrita).

**Recuperação.** Um Barrel iniciado com `--peer` pede `getSnapshot` ao par (streaming das mensagens aplicadas) e reaplica-as; o resto chega pelas retransmissões do Downloader.

**Fila de URLs.** Cada URL é entregue a um só Downloader e tem um *lease* de 60 s: se não for confirmado com `done`, volta à fila, por isso a falha de um Downloader não perde indexações.

## 3. Componente RPC e failover

**Gateway:** `putNew`, `putMany`, `takeNext`, `done` (fila de URLs); `searchWords`, `getBacklinks`; `getStats` (streaming); `registerBarrel`, `updateIndexSize`.
**Barrel:** `addToIndex`, `searchWords`, `getBacklinks`, `getSnapshot` (streaming).

A Gateway cria um stub para o endereço que cada Barrel comunica em `registerBarrel`. Cada pesquisa percorre os Barrels em *round-robin*: se um responde com `RpcError`, é removido e o pedido repete-se no seguinte; sem Barrels, devolve `UNAVAILABLE`. Pagina os resultados de 10 em 10 e conta as pesquisas. Não usámos *callbacks* tipo RMI: as estatísticas (10 pesquisas mais comuns, Barrels ativos com o tamanho do índice, tempo médio por Barrel em décimas de segundo) seguem em *streaming* servidor→cliente, só quando mudam.

**Failover.**

- *Barrel:* regista-se de 5 em 5 s (a Gateway ignora repetições), por isso volta sozinho se tiver sido removido.
- *Downloader:* o URL não confirmado volta à fila ao fim de 60 s; falhas da Gateway levam a novas tentativas.
- *Gateway:* o estado (fila, Barrels, contagens) é guardado em `gateway_state.json` e recuperado no arranque.
- *Client:* repete cada chamada (`retries`, `retry_delay`) e reabre o *stream* de estatísticas.

## 4. Distribuição de tarefas

| Elemento | Responsabilidades |
| --- | --- |
| João Leitão | Downloaders, parser, multicast fiável, Barrel e armazenamento, fila de URLs |
| Dinis Maia | Gateway, Client, estatísticas, failover da Gateway, `config.properties`, base do `googol.proto` |

## 5. Testes

| Teste | Resultado |
| --- | --- |
| Indexar URL introduzido no client | Pass |
| Consultar páginas com ligação para um URL | Pass |
| Estatísticas atualizam no client | Pass |
| Dois downloaders visitam URL's diferentes | Pass |
| Ao fechar um Barrel, a pesquisa continua no outro | Pass |
| Durante indexação Barrel está em baixo, ao voltar iguala ao outro | Pass |
| Ao reiniciar Gateway recupera estado e Client continua a funcionar | Pass |
