# ShopVibe

Pipeline local de e-commerce com PostgreSQL, Debezium/Kafka, MinIO e Python.

As decisoes arquiteturais estao registradas em [docs/adr](docs/adr/README.md), com tres ADRs em Word e Markdown: captura CDC, transporte Kafka e armazenamento no MinIO. Os registros distinguem a implementacao local das propostas para producao e documentam a pendencia de licenciamento do sink S3.

## Iniciar o projeto completo

Com Docker Desktop ativo, na pasta deste README:

```powershell
.\start-shopvibe.ps1
```

Pre-requisitos: Docker Desktop ativo, PowerShell e acesso a internet na primeira instalacao. O script sobe o Compose, baixa o abctl com verificacao SHA256, instala Airbyte 1.8.5 (chart 1.8.5), configura as duas fontes e o destino MinIO, sincroniza Bling/Melhor Envio e valida Bronze/Silver/Gold. A primeira instalacao baixa diversas imagens e pode levar ate 30 minutos. Airbyte recomenda pelo menos 8 GB de memoria para ele; para esta stack completa, reserve preferencialmente 12 a 16 GB ao Docker. O instalador usa o modo de poucos recursos e sincronizacoes sequenciais.

Nao e necessario criar venv, preencher senhas em JSONs nem executar o registro manual do CDC. O Compose tem valores de desenvolvimento e usa o `.env` existente quando presente. O `.env` guarda a configuracao local e nao entra no Git; `.env.example` e o modelo versionado, sem credenciais reais. Para personalizar uma nova instalacao, copie `.env.example` para `.env` antes da primeira subida. As credenciais de exemplo sao apenas para ambiente local.

O Airbyte atual usa Kubernetes local dentro do Docker, administrado pelo abctl; por isso ele nao aparece como um servico unico deste Compose. `docker compose up -d --build` inicia os demais componentes, mas uma instalacao nova precisa de `start-shopvibe.ps1` para preparar tambem o Airbyte. O script pode ser repetido: reaproveita as conexoes existentes e preserva alteracoes feitas pela interface. Cada execucao completa gera um novo pedido de demonstracao. Use `-SkipSync` para apenas preparar os servicos/conexoes, sem executar a coleta Airbyte e a Silver.

A inicializacao executa:

1. PostgreSQL com WAL logico e banco `shopvibe_db`.
2. Schema `shopvibe` com tabelas `customers`, `products` e `orders`.
3. Cadastro do PostgreSQL no pgAdmin, com usuario e arquivo de senha gerados a partir das mesmas variaveis do Compose.
4. Registro do `connector-shopvibe-cdc`, filtrado para essas tres tabelas.
5. Instalacao do plugin S3 10.6.11 com checksum verificado, criacao do bucket `bronze` com versionamento e registro do `connector-minio-bronze`.

O `postgres-init`, `minio-init` e `connector-init` devem terminar com `Exited (0)`: sao servicos de inicializacao, nao processos permanentes. Kafka Connect e `connector-watch` devem permanecer ativos. O registro dos dois conectores fica centralizado em `docker/bootstrap/register_connector.py`. Para reaplica-los manualmente com a infraestrutura ativa, execute `docker compose run --rm connector-init`.

## Acessos

| Servico | Endereco | Credenciais padrao (sem .env) |
|---|---|---|
| pgAdmin | http://localhost:5050 | admin@shopvibe.com / admin |
| PostgreSQL | localhost:5432, banco shopvibe_db | postgres / postgres |
| Kafka Connect | http://localhost:8083/connectors | Sem autenticacao neste laboratorio |
| MinIO | http://localhost:9002 | admin / minioadmin |
| Airflow | http://localhost:8080 | admin / admin |
| Airbyte | http://localhost:8001 | Geradas automaticamente; consulte abaixo |
| APIs simuladas | http://localhost:8002/health | Sem autenticacao neste laboratorio |

Para consultar o login local do Airbyte:

```powershell
Get-Content .\.runtime\airbyte-login.txt
```

O instalador identifica Windows AMD64/ARM64. Credenciais usadas na automacao e IDs das conexoes ficam em `.runtime/airbyte.json`, ignorado pelo Git. O email inicial e `admin@shopvibe.com` e a senha e gerada pelo Airbyte; uma organizacao ja configurada conserva seu email. Dados da plataforma ficam no ambiente persistente do abctl, separado dos volumes do Compose.

No pgAdmin, abra `ShopVibe > ShopVibe PostgreSQL > Databases > shopvibe_db > Schemas > shopvibe > Tables`. A senha da conexao ao banco ja vem configurada; o login da interface continua necessario.

## Volumes existentes

O SQL em `docker/postgres/01-schema.sql` e montado em `/docker-entrypoint-initdb.d`, executado pelo PostgreSQL apenas quando o volume esta vazio. O servico `postgres-init` aplica o mesmo SQL tambem a volumes existentes.

As instrucoes `CREATE ... IF NOT EXISTS` preservam os registros existentes. O bootstrap nao altera senhas de usuarios de um PostgreSQL ja inicializado: mantenha no `.env` a senha real desse volume. Alterar o `.env` nao equivale a executar `ALTER ROLE` no banco. Tabelas existentes com outra estrutura exigem uma migracao propria; o init nao as recria.

As tabelas do Airflow em `public` permanecem intactas e o CDC passa a ler somente `shopvibe`. Os topicos antigos do Kafka permanecem disponiveis, mas nao sao alimentados por esse filtro.

Kafka e ZooKeeper usam volumes nomeados nesta versao. A migracao do ambiente local ja foi concluida; seus comprovantes e o backup do pgAdmin permanecem em `.local-backup`, fora do Git. Uma instalacao nova nao precisa executar migracao.

## Testar o CDC

No Query Tool do banco `shopvibe_db`, execute (use IDs novos ao repetir o teste):

```sql
INSERT INTO shopvibe.products (id, name, price, stock)
VALUES ('TESTE-001', 'Produto de teste', 10.00, 1);

UPDATE shopvibe.products SET price = 12.00 WHERE id = 'TESTE-001';
DELETE FROM shopvibe.products WHERE id = 'TESTE-001';
```

No terminal:

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic shopvibe.shopvibe.products --from-beginning --max-messages 3 --timeout-ms 15000
```

Os eventos CDC possuem `payload.op` igual a `c`, `u` e `d`. `--from-beginning` inclui eventos de testes anteriores. Os nomes seguem `prefixo.schema.tabela`, por isso o novo topico e `shopvibe.shopvibe.products`.

## Bronze no MinIO

O sink grava os tres topicos CDC e continua preservando o topico legado no bucket `bronze`:

- `shopvibe.shopvibe.customers`
- `shopvibe.shopvibe.products`
- `shopvibe.shopvibe.orders`
- `shopvibe.orders.raw`

Novos checkouts entram exclusivamente pelo PostgreSQL e aparecem em `shopvibe.shopvibe.orders`. O topico `shopvibe.orders.raw` permanece configurado para conservar e permitir a leitura dos eventos antigos; a API nao publica mais nele.

Abra http://localhost:9002, entre com as credenciais do `.env` (ou admin/minioadmin no padrao) e navegue em `bronze > topics > nome-do-topico > partition=0`. Os arquivos `.json` usam o offset no nome. O JSON conserva o evento de origem, acrescido de `_kafka_topic`, `_kafka_partition`, `_kafka_offset` e `_kafka_timestamp` para rastreabilidade.

Insercoes, atualizacoes e exclusoes CDC ficam como eventos separados; excluir uma linha do PostgreSQL nao apaga o historico da Bronze. O envelope de exclusao (`op=d`) e preservado. Somente os tombstones Kafka, que possuem valor nulo e servem a compactacao, sao ignorados pelo sink.

`flush.size=1` grava cada evento sem aguardar um lote de tres mensagens. Essa escolha favorece a demonstracao e a confirmacao imediata de entrega, ao custo de muitos arquivos pequenos.

Para comparar todo o Kafka retido com os arquivos atuais da Bronze, sem alterar offsets de consumidores:

```powershell
.\.venv\Scripts\python.exe .\verify-bronze.py
```

A auditoria verifica payloads completos, offsets ausentes e duplicados nos quatro topicos. O relatorio fica em `.local-backup/bronze-validation.json`; sem eventos, o teste nao e considerado aprovado.

## Persistencia e recuperacao

Kafka, ZooKeeper, PostgreSQL e MinIO usam volumes nomeados. Kafka foi configurado sem expiracao automatica por tempo ou tamanho, e os topicos de negocio usam limpeza por exclusao, sem compactacao do historico. O bucket tem versionamento e nenhuma regra de expiracao e criada pelo projeto. Acompanhe o espaco disponivel: esse historico cresce continuamente.

O sink nao ignora erros de dados (`errors.tolerance=none`). Se o MinIO ficar indisponivel, os eventos permanecem no Kafka ate serem gravados. `connector-watch` tenta reiniciar tasks FAILED quando suas dependencias voltam, preservando offsets; erros persistentes continuam visiveis em `/connectors/{name}/status`. Configuracoes/offsets do Connect tambem ficam nos topicos persistidos do Kafka.

Para parar e retomar preservando dados:

```powershell
docker compose stop
docker compose up -d
```

**Nao use `docker compose down --volumes` para reiniciar: esse comando apaga os dados.** Recriar o ambiente do zero e um teste destrutivo, diferente de testar a retomada. Um broker e um MinIO locais nao protegem contra perda do disco; essa protecao exige backup externo/replicacao.

## API, consumidor e simulador

A API e executada pelo Compose. A imagem `api/Dockerfile` instala `api/requirements.txt`; nao e necessario criar venv para executar os containers. O caminho do checkout e unico: **API -> PostgreSQL -> Debezium -> Kafka -> conector S3 -> MinIO Bronze**.

O servico `simulador` prepara somente cliente e produto ficticios no PostgreSQL, depois envia o pedido para `POST /api/v1/checkout`. Somente a API insere o pedido. O simulador nao altera estoque nem publica no Kafka e deve terminar com `Exited (0)`. Para acompanhar:

```powershell
docker compose logs --tail 50 api simulador
```

Para desenvolver com Python local, opcionalmente:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\api\requirements.txt
.\.venv\Scripts\python.exe -m uvicorn main:app --app-dir .\api --reload
```

A API confirma o commit em `shopvibe.orders` antes de responder `201`. Debezium publica a mudanca em `shopvibe.shopvibe.orders`, e o sink grava em `bronze/topics/shopvibe.shopvibe.orders/`. Essa entrega e assincrona: sucesso HTTP comprova a persistencia no banco; a auditoria comprova a chegada na Bronze. Se Kafka/MinIO estiverem temporariamente fora, o checkout continua dependendo do PostgreSQL; o CDC retoma a captura e entrega quando os servicos voltam.

Cliente e produtos precisam estar cadastrados. Quantidades devem ser positivas, precos ter no maximo duas casas decimais e o total deve corresponder aos itens. Repetir o mesmo `order_id` com os mesmos dados retorna `200`, sem inserir/atualizar o pedido nem gerar outro evento CDC; outro conteudo para o mesmo ID retorna `409`. Cliente inexistente retorna `404`, produto/valores invalidos retornam `422` e falha de persistencia retorna `503`. Em caso de erro de rede/503, repita com o mesmo ID. O horario da primeira criacao e mantido. O status ainda segue o contrato de simulacao existente; a autorizacao de pagamento sera implementada separadamente.

Para gerar outro checkout pela API:

```powershell
docker compose run --rm simulador
```

Para testar API, commit, repeticao, rejeicoes e chegada do CDC na Bronze com a stack ativa:

```powershell
Get-Content -Raw .\tests\test_checkout_integration.py | docker compose exec -T -e SHOPVIBE_INTEGRATION=1 api python -
```

O teste cria dados ficticios e os conserva no banco/Bronze. A auditoria `verify-bronze.py` continua comparando todos os eventos Kafka retidos com o MinIO.

O `consumer.py`/`ingestao.py` antigo permanece apenas para compatibilidade com `shopvibe.orders.raw`, no profile opcional `legacy` (`docker compose --profile legacy up -d consumer`). Ele grava em `shopvibe-bronze`, que nao e o destino do checkout novo. Nao e necessario executa-lo para a entrega CDC na Bronze; dados antigos, offsets e buckets sao preservados.

Os dois JSONs em `conectores/` definem o CDC e o sink MinIO. Os campos vazios de credenciais sao preenchidos pelo servico `connector-init` a partir da configuracao do Compose.

## Silver, Gold, servicos simulados e agendas

Os JSONs em `samples/services` simulam API de pagamento, ClearSale, Bling e Melhor Envio, acompanhados de clientes/produtos/pedidos ficticios relacionados. Bling e Melhor Envio sao publicados por `mock-services` e coletados de verdade pelo Airbyte, com conectores declarativos. O destino S3 grava JSONL nativo em `bronze/airbyte/` no MinIO, em modo **Full refresh | Append**, sem sobrescrever as coletas anteriores. A Silver le esse envelope e os dados CDC, preservando os objetos Bronze.

```powershell
docker compose exec airflow python -m pipelines.cli demo
docker compose exec airflow python -m pipelines.cli verify
docker compose exec airflow python -m pipelines.cli verify-gold
```

No Airflow existem somente duas DAGs: `shopvibe_servicos_4h` dispara os jobs Airbyte de Bling e Melhor Envio sequencialmente a cada quatro horas UTC e verifica a chegada da versao atual dos exemplos na Bronze; `shopvibe_silver_diaria` processa Silver e depois Gold diariamente as 02:00 UTC (23h do dia anterior em Sao Paulo). Suas quatro tasks sao `build_silver`, `validate_silver`, `build_gold` e `validate_gold`. ClearSale e pagamento nao sao orquestrados pelo Airflow no diagrama. Seus JSONs sao apenas exemplos para testar a modelagem; o acionamento Kafka -> ClearSale e a integracao operacional de pagamento ainda nao foram implementados. Os servicos externos continuam simulados; chamadas aos fornecedores reais e Spark ainda nao fazem parte desta etapa.

O comando `demo` prepara somente clientes/produtos/pedidos, pagamentos e ClearSale a partir dos arquivos. Ele exige que a coleta Airbyte de Bling/Melhor Envio ja esteja confirmada e nao copia esses arquivos diretamente para contornar o Airbyte.

Consulte [Airbyte: configuracao, teste e operacao](docs/AIRBYTE.md) para administrar os conectores pela interface e testar mudancas nas APIs simuladas.

No MinIO, consulte `silver/LATEST.json` e o manifesto do snapshot indicado. A [modelagem Silver](docs/SILVER.md) explica grano, chaves, tipos, exclusoes, qualidade, procedencia e os testes.

A [modelagem Gold](docs/GOLD.md) gera sete tabelas no bucket `gold`: dimensoes de cliente, produto e data; fatos de pedidos e itens; vendas diarias por status/moeda/simulacao; e ultimo saldo de estoque por produto/deposito. Os indicadores representam valores de pedidos, nao pagamento recebido. Para construir a Gold da Silver atual:

```powershell
docker compose exec -T airflow python -m pipelines.cli build-gold
docker compose exec -T airflow python -m pipelines.cli verify-gold
```

Consulte `gold/LATEST.json` e seu manifesto para localizar os Parquets. Cada Gold identifica a Silver de origem, reconcilia os valores e preserva snapshots anteriores. Essa carga e em lote; o MVP obrigatorio continua sendo a captura de pedidos ate Kafka/Bronze, e nao depende de Gold, Airbyte ou integracao de pagamento.

Referencias: [inicializacao PostgreSQL](https://hub.docker.com/_/postgres), [configuracao automatica do pgAdmin](https://www.pgadmin.org/docs/pgadmin4/latest/container_deployment.html), [configuracao S3 Sink](https://docs.confluent.io/kafka-connectors/s3-sink/current/configuration_options.html).
