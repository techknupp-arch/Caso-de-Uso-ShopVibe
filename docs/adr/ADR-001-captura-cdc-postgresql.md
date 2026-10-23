# ADR-001 Captura de pedidos com CDC no PostgreSQL

**Status:** Aceito  
**Data:** 21/10/2026  
**Autores:** Daniel e Jessica Fernanda Knupp

Adotamos o Debezium para publicar no Kafka as alterações confirmadas no PostgreSQL. O banco é a origem dos pedidos; a captura pelo log transacional atende ao RF01 sem exigir uma publicação adicional na API.

## Contexto

O estudo de caso exige que um novo pedido chegue ao Kafka em poucos segundos. Publicar no banco e no Kafka em duas operações independentes criaria uma janela de inconsistência se apenas uma delas concluísse. A captura precisa também permitir retomada após interrupções.

No projeto, o checkout grava em shopvibe.orders. Clientes e produtos completam o contexto do pedido. Pagamentos, antifraude e logística reais são componentes da arquitetura futura; não são pré-requisitos para demonstrar esse caminho.

## Decisão

Usar o conector PostgreSQL do Debezium no Kafka Connect, com wal_level=logical, plugin pgoutput e slot debezium_shopvibe. Capturar somente shopvibe.customers, shopvibe.products e shopvibe.orders; excluir as tabelas de metadados do Airflow.

O fluxo implementado é checkout → commit no PostgreSQL → Debezium → Kafka. O tópico de pedidos é shopvibe.shopvibe.orders. A resposta de sucesso da API confirma a gravação no banco; a chegada ao Kafka e à Bronze é assíncrona e deve ser verificada separadamente.

## Alternativas consideradas

| Alternativa | Vantagens | Desvantagens |
|---|---|---|
| CDC com Debezium | Captura inserções, alterações e exclusões sem dupla publicação pela API. | Exige replicação lógica, permissões e acompanhamento do WAL. |
| Outbox transacional | Registra a intenção do evento na mesma transação; favorece eventos de negócio. | Acrescenta tabela, publicador e tratamento do ciclo de vida dos eventos. |
| Consulta periódica | Implementação inicial simples, baseada em SQL. | Depende de checkpoints e consultas recorrentes; alterações e exclusões exigem controle adicional. |

## Consequências

Ganhamos desacoplamento entre checkout e consumidores, rastreabilidade da origem e captura contínua. Em contrapartida, os eventos refletem o modelo físico do banco; mudanças de schema exigem compatibilidade nos consumidores.

Se o conector parar, o slot pode reter WAL e ocupar o disco do PostgreSQL. A operação deve acompanhar atraso da captura, espaço livre e estado das tarefas. Apagar slots ou offsets para resolver falhas pode comprometer a retomada.

## Durabilidade e limites atuais

A retomada depende do WAL ainda disponível, do slot e dos offsets persistidos. No laboratório, volumes Docker preservam dados entre reinícios, mas uma única instância não protege contra perda do disco ou do computador.

Para produção, propomos backup com restauração testada e uma estratégia de replicação e continuidade do PostgreSQL que preserve a captura lógica. Essas medidas não estão implementadas nem foram demonstradas por teste de falha.

## Escalabilidade

O cenário de Black Friday prevê até 20 vezes a demanda, cerca de 300 mil pedidos por dia. O conector PostgreSQL usa uma tarefa; aumentar tasks.max não paraleliza essa leitura. O dimensionamento deve medir WAL, atraso e taxa de eventos, que pode superar a taxa de pedidos. O paralelismo após a captura é tratado no ADR-002. Não realizamos teste de carga.

## Custos e licenciamento

PostgreSQL e Debezium são ferramentas de código aberto. O custo operacional inclui disco para WAL, processamento e monitoramento. O bootstrap configura o schema e registra o conector; não há uma etapa manual de publicação de cada pedido.

## Implementação e evidências

Implementação: api/main.py, conectores/connector-shopvibe-cdc.json e docker-compose.yml. A validação local registrada em 21/10/2026 confirmou o percurso de checkout e CDC. Este registro documenta essa evidência; não constitui uma nova medição de latência ou disponibilidade.

## Referências

- [Debezium e PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Replicação lógica no PostgreSQL](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)
