# Modelagem Silver e simulacao dos servicos

## Escopo desta etapa

O fluxo segue os servicos e os horarios do diagrama: API de pagamento, ClearSale (antifraude), Bling (estoque), Melhor Envio (logistica), cargas dos dois ultimos a cada quatro horas e Silver diariamente as 02:00 UTC. O Kafka continua alimentando a Bronze em streaming.

Nesta implementacao, as APIs simuladas de Bling e Melhor Envio sao coletadas pelo Airbyte e gravadas no MinIO como JSONL nativo em `bronze/airbyte/`. Python prepara os demais exemplos. Nao ha chamadas autenticadas aos fornecedores reais. O processamento Silver usa Python/PyArrow e grava Parquet, alimentando a Gold de vendas e ultimo saldo de estoque. Spark, DW em outro banco, mart de fraude, Power BI e processamento em streaming depois da Bronze ficam como evolucao. Veja a [operacao do Airbyte](AIRBYTE.md).

## JSONs de entrada

Os sete arquivos em `samples/services` sao totalmente ficticios:

| Fonte | Arquivo | Conteudo |
|---|---|---|
| ClearSale | `clearsale/fraud_assessments.json` | 3 respostas de analise, com codigo do pedido, status e score |
| API de pagamento | `payment_api/payments.json` | 3 pagamentos relacionados aos pedidos; contrato generico de demonstracao |
| Bling | `bling/inventory.json` | 3 produtos x 2 depositos, saldo fisico e virtual |
| Melhor Envio | `melhor_envio/shipments.json` | 2 envios, incluindo um postado e um entregue |
| ShopVibe demonstracao | `shopvibe_demo/customers.json` | 2 clientes ficticios |
| ShopVibe demonstracao | `shopvibe_demo/products.json` | 3 produtos ficticios |
| ShopVibe demonstracao | `shopvibe_demo/orders.json` | 3 pedidos e 4 itens relacionados aos servicos |

Cada arquivo tem um envelope nosso (`contract_version`, `source_system`, `dataset`, `simulated`, `extracted_at`, `references`, `body`). O `body` representa uma resposta simplificada do servico; o envelope e os mapas de IDs sao metadados da integracao, nao campos oficiais dos fornecedores.

Os nomes de campos da ClearSale e os conceitos de saldo do Bling foram baseados nas documentacoes oficiais, mas estes exemplos nao equivalem a uma homologacao de API. Para Melhor Envio, o exemplo e um contrato simplificado de rastreio que precisara ser conciliado com a resposta real na integracao.

- [ClearSale Realtime Decision e consulta de status](https://api.clearsale.com.br/docs/realtimedecision)
- [Bling: produto, deposito e saldos](https://developer.bling.com.br/webhooks)
- [Melhor Envio: consulta de status das etiquetas](https://docs.melhorenvio.com.br/reference/rastreio-de-envios)

A ClearSale informa na documentacao que seu webhook de mudanca de status nao leva o status: ele exige consulta posterior a API. O JSON desta simulacao representa a resposta com o status, nao o webhook. O score e preservado sem atribuir classes de risco ou faixas nao definidas pelo negocio.

## Grano e chaves

A Silver e normalizada por entidade. O modelo dimensional de vendas e ultimo saldo de estoque esta implementado na [Gold](GOLD.md), que le uma versao validada desta Silver. O mart de fraude fica como evolucao.

| Tabela Parquet | Grano/chave | Campos de negocio principais |
|---|---|---|
| `customers` | 1 estado atual por `customer_id` | name, email |
| `products` | 1 estado atual por `product_id` | name, price, stock |
| `orders` | 1 estado atual por `order_id` | customer_id, total_amount, currency, payment_method, status, ordered_at |
| `order_items` | 1 estado por `order_id + line_number` | product_id, quantity, unit_price, line_amount |
| `payments` | 1 estado mais recente por `payment_id` | order_id, status, amount, currency, method, processed_at |
| `fraud_assessments` | 1 decisao observada mais recente por `order_id` | status_code, score, observed_at |
| `inventory` | 1 posicao mais recente por `product_id + warehouse_id` | physical_stock, virtual_stock, stock_at |
| `shipments` | 1 estado mais recente por `shipment_id` | order_id, status, tracking_code, carrier, protocol, posted_at, delivered_at |
| `event_history` | 1 evento unico por `source_event_id` | entity, entity_id, record_json e procedencia |

`line_number` e a posicao do item dentro do pedido, pois as entradas atuais nao fornecem um ID imutavel da linha. A remocao de um item ou pedido marca as linhas como excluidas; o evento anterior permanece no historico.

Valores monetarios e score usam `DECIMAL(18,2)`; quantidades do Bling usam `DECIMAL(18,4)` para suportar unidades fracionarias. Datas usam timestamp com timezone UTC. IDs sao strings. A moeda dos pedidos e BRL neste contrato, pois o schema/API atuais nao informam moeda. `physical_stock` e `virtual_stock` sao mantidos separadamente, sem inferir regras de reserva nao confirmadas.

## Procedencia e qualidade

As tabelas incluem `source_system`, `source_event_id`, `event_at`, `source_object`, `source_sequence`, `is_deleted`, `is_simulated` e `quality_flags`.

- Kafka: ID do evento = topico + particao + offset. O offset define a ordem dentro da origem atual. Os topicos deste projeto possuem uma particao.
- Arquivos externos: ID do evento deriva de fonte, entidade, instante de extracao e registro normalizado. A coleta repetida do mesmo evento nao duplica o historico.
- `event_at` usa o timestamp do CDC/API quando disponivel; nas respostas externas sem horario de negocio, usa o horario da extracao. `observed_at` da ClearSale e o horario da consulta simulada, nao a hora comprovada da decisao.
- Timestamps sem timezone dos checkouts antigos sao interpretados como UTC. Novos checkouts sao persistidos no PostgreSQL e chegam pelo CDC, com `created_at` do pedido.
- O preco/total Decimal do Debezium e decodificado a partir do valor binario e da escala do schema; o campo JSONB `items` e convertido de string JSON em itens tipados.
- Para o mesmo pedido/ID, PostgreSQL tem prioridade sobre a API; a API tem prioridade sobre os dados de demonstracao. As outras versoes continuam em `event_history`.
- Exclusoes mantem a ultima imagem conhecida com `is_deleted=true`. As contagens fisicas incluem esses registros; filtre `is_deleted=false` para consultar apenas os ativos.
- Relacionamentos ausentes e divergencias entre total e itens geram flags; a linha e preservada, sem inner joins que eliminem dados.
- Registros ilegais ou eventos com o mesmo ID e conteudos divergentes impedem a publicacao. Os erros sao registrados em `silver/quarantine/<id>/rejections.json`, com referencia aos objetos originais. A versao anterior continua acessivel.

Os campos nao modelados continuam nos JSONs originais da Bronze. A Silver nao remove nem modifica esses arquivos.

## Publicacao e leitura

```text
bronze/topics/...                  eventos Kafka originais
bronze/external/<fonte>/<dataset>/<sha256>.json
silver/snapshots/<id>/<tabela>/part-00000.parquet
silver/snapshots/<id>/manifest.json
silver/LATEST.json
silver/quarantine/<id>/rejections.json
```

Os arquivos externos sao copiados byte a byte para chaves por hash. Uma nova exportacao permanece como outro objeto; repetir o mesmo arquivo apenas verifica e reutiliza o existente.

Cada snapshot e calculado a partir dos objetos Bronze disponiveis na leitura e da versao do codigo. O processo baixa e verifica os Parquets, suas contagens, tipos, chaves e checksums antes de atualizar o unico ponteiro `LATEST.json`. Snapshots anteriores permanecem no bucket versionado. Uma falha de escrita deixa o ponteiro anterior intacto.

**Consulte as tabelas indicadas pelo manifesto apontado por `LATEST.json`.** Ler recursivamente todos os Parquets de todos os snapshots duplicaria versoes completas dos dados.

Este caso de uso faz reconstruicao integral a partir da Bronze. Nao e um motor transacional/incremental nem um snapshot consistente de todas as transacoes entre servicos. Os manifestos registram quais objetos entraram em cada versao; dados que chegarem depois entram na proxima carga. Execute um escritor Silver por vez; a DAG usa `max_active_runs=1`.

## Agendas do Airflow

| DAG | Cron UTC | Acao |
|---|---|---|
| `shopvibe_servicos_4h` | `0 */4 * * *` | Dispara jobs Airbyte de Bling/Melhor Envio e verifica a entrega na Bronze |
| `shopvibe_silver_diaria` | `0 2 * * *` | Constroi e valida o snapshot Silver |

02:00 UTC = 23:00 do dia anterior em America/Sao_Paulo. As cargas de quatro horas ocorrem as 00, 04, 08, 12, 16 e 20 UTC. `catchup=False` evita criar todas as execucoes antigas; ao ativar uma DAG, o Airflow pode selecionar o ultimo intervalo concluido. As DAGs estao habilitadas no ambiente local.

A carga Silver usa o que esta disponivel na execucao; o intervalo do Airflow nao e um filtro historico de competencia. Para acrescentar um periodo de simulacao, adicione outro JSON com `extracted_at` e dados atualizados. Reexecutar as amostras estaticas nao inventa novas operacoes nem novos arquivos iguais.

## Repetir o teste

Na pasta do Compose:

```powershell
.\start-shopvibe.ps1
docker compose exec airflow python -m pipelines.cli demo
docker compose exec airflow python -m pipelines.cli verify
```

`demo` confirma que as duas exportacoes Airbyte atuais chegaram, carrega os cinco JSONs restantes (base ficticia, pagamentos e ClearSale) e constroi a Silver. Os exemplos de pagamento e ClearSale nao representam chamadas operacionais nem passam por DAGs proprias. `verify` le novamente os objetos publicados e valida o snapshot. Para testar as duas DAGs, abra http://localhost:8080 e use Trigger DAG. A DAG Silver possui as tasks `build_silver` e `validate_silver`; ambas precisam terminar em success.

No MinIO em http://localhost:9002, abra `silver/LATEST.json`, copie o `snapshot_id` e navegue ate `silver/snapshots/<snapshot_id>/`. Os JSONL de Bling/Melhor Envio ficam em `bronze/airbyte/`; os demais exemplos ficam em `bronze/external/`. Os arquivos antigos em `external/bling` e `external/melhor_envio` permanecem preservados e eventos iguais sao deduplicados na Silver.

Para executar localmente, fora do Airflow:

```powershell
.\.venv\Scripts\python.exe -m pip install -r pipelines\requirements.txt
.\.venv\Scripts\python.exe -m pipelines.cli demo
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## Validacao realizada

Nove testes cobrem Decimal do CDC, JSONB, ordenacao por offset mesmo com regressao de relogio, exclusoes, remocao de itens, deduplicacao/conflitos, chaves divergentes na mesma exportacao, grano/relacionamentos, idempotencia, recuperacao do ponteiro, corrupcao de arquivo, quarentena e falha parcial sem trocar LATEST.

Cinco testes adicionais cobrem JSONL/gzip do Airbyte, deduplicacao entre exportacoes antigas e coletas novas, quarentena sem substituir LATEST, verificacao dos exemplos atuais, retomada de jobs ativos e bloqueio da copia direta de Bling/Melhor Envio fora do Airbyte.

Na validacao inicial, antes do Airbyte, sete JSONs foram ingeridos e a Silver reunia 33 eventos: 22 dos exemplos e 11 previamente existentes. Essas contagens sao historicas: novos pedidos CDC/API e novas respostas podem aumenta-las. Os exemplos isolados possuem 2 clientes, 3 produtos, 3 pedidos, 4 itens, 3 pagamentos, 3 analises, 6 saldos, 2 envios e 22 eventos. A ingestao Airbyte conserva esse grano e deduplica exportacoes repetidas, preservando cada arquivo bruto.

A leitura dos Parquets foi validada tanto no Python local/PyArrow 17 quanto no Airflow/PyArrow 11. As DAGs manuais antigas para carregar exemplos ClearSale/pagamentos foram retiradas para alinhar ao diagrama; o Airflow fica responsavel por coletas Bling/Melhor Envio e pela sequencia Silver -> Gold. Os horarios automaticos estao configurados; execucao manual nao comprova que ja chegou o proximo horario agendado.
