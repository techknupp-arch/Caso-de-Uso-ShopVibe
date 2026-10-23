# ADR-002 Transporte e retenção de eventos com Kafka

**Status:** Aceito  
**Data:** 21/10/2026  
**Autores:** Daniel e Jessica Fernanda Knupp

Adotamos o Apache Kafka como log de eventos entre a captura CDC e os consumidores. Essa escolha permite reprocessamento e consumo independente, enquanto o MinIO mantém o histórico bruto descrito no ADR-003.

## Contexto

RF01 exige transportar novos pedidos rapidamente; RF02 exige preservar os eventos brutos. Consumidores podem parar ou ter velocidades distintas. Uma entrega vinculada diretamente ao checkout dificultaria desacoplar essas falhas e repetir processamentos.

O desenho prevê ramificações para antifraude, estoque, logística e armazenamento. Cada integração que precisar de todos os eventos deverá ter seu próprio grupo de consumo. Offsets são posições de leitura em uma partição, não identificadores de destino.

## Decisão

Usar Kafka e Kafka Connect no Docker Compose. O laboratório executa um broker e ZooKeeper, com imagens Confluent 7.3.0, volumes persistentes e fator de replicação 1. A configuração atual não usa KRaft.

Os novos pedidos chegam a shopvibe.shopvibe.orders pelo Debezium. O tópico shopvibe.orders.raw permanece como histórico do fluxo anterior. A retenção por tempo e por tamanho está desabilitada com os dois limites em -1; a política de limpeza é delete.

O consumidor da Bronze mantém seus offsets. Uma interrupção permite retomar a leitura enquanto os eventos estiverem disponíveis. Não declaramos garantia de processamento exatamente uma vez de ponta a ponta.

## Alternativas consideradas

| Alternativa | Vantagens | Desvantagens |
|---|---|---|
| Apache Kafka | Log persistente, replay por offset e grupos independentes; integração com Connect. | Exige operação do cluster, armazenamento e controle de capacidade. |
| RabbitMQ | Filas duráveis e confirmações atendem bem à distribuição de tarefas. | Filas clássicas removem mensagens confirmadas; replay requer outra estratégia, como Streams. |
| Redis Streams | Oferece grupos de consumo e leitura de histórico com baixa latência. | Persistência, retenção e recuperação exigem configuração e dimensionamento próprios. |

## Consequências

A captura continua independente do ritmo da Bronze, e novos consumidores podem reler o histórico. O custo é manter mais componentes, acompanhar o atraso dos grupos e gerenciar disco, memória e falhas de conectores.

A retenção sem expiração evita descarte automático no laboratório, mas o disco continua finito. Antes de adotar uma janela de retenção em produção, será preciso conferir o arquivamento na Bronze e a janela necessária de recuperação. Não há descarte automático implementado hoje.

## Durabilidade e limites atuais

Volumes persistentes permitem recuperação após reinícios com o armazenamento intacto. Um broker com replicação 1 fica indisponível quando essa instância falha e não tolera a perda de seu disco. Persistência local não equivale a alta disponibilidade.

Para produção, propomos ao menos três brokers, replicação 3, min.insync.replicas=2 e confirmação acks=all no produtor. Essa combinação deve ser validada com monitoramento e testes de recuperação. Ela não está configurada no Compose atual.

## Escalabilidade

Para o pico previsto de 20 vezes a demanda, propomos dimensionar partições, disco e rede a partir do tamanho e da taxa reais dos eventos. Mais partições permitem mais consumidores ativos por grupo; a chave estável do pedido preserva sua ordem dentro de uma partição. Não há ordem global entre partições. Os 300 mil pedidos por dia são uma meta de dimensionamento, não capacidade comprovada por teste.

## Custos e licenciamento

Apache Kafka e ZooKeeper têm licença Apache 2.0. Os plugins e componentes distribuídos nas imagens devem ser avaliados separadamente; a licença do conector S3 está registrada no ADR-003. Não há cobrança de serviço gerenciado neste laboratório, mas existem custos de infraestrutura e operação.

## Implementação e evidências

Implementação: docker-compose.yml e os dois JSONs em conectores/. A auditoria local de 21/10/2026 conciliou 52 eventos esperados com 52 eventos na Bronze, sem ausências, divergências ou duplicações naquela amostra. Isso não demonstra tolerância a falhas nem desempenho em carga.

## Referências

- [Projeto e documentação do Apache Kafka](https://kafka.apache.org/documentation/)
- [Licença Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
