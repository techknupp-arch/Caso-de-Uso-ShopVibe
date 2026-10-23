# ADR-003 Histórico bruto e camadas de dados no MinIO

**Status:** Aceito  
**Data:** 21/10/2026  
**Autores:** Daniel e Jessica Fernanda Knupp

Adotamos o MinIO como armazenamento de objetos para preservar a Bronze e separar os dados derivados em Silver e Gold. A escolha atende ao histórico para reprocessamento do RF02; a licença do conector S3 atual permanece uma pendência de adequação ao enunciado.

## Contexto

O histórico precisa sobreviver aos processamentos e permitir reconstruir camadas analíticas. Guardar apenas a situação atual de um pedido perderia a sequência das alterações. Usar o banco transacional como arquivo permanente também aumentaria o acoplamento com as consultas operacionais.

A demonstração ocorre localmente com Docker. Bronze persistida é obrigatória; Silver e Gold são extensões já implementadas em lote. Pagamento real, painéis e consumo analítico em streaming continuam como evolução da arquitetura.

## Decisão

Usar buckets bronze, silver e gold no MinIO, com versionamento, volume persistente e sem regra de expiração configurada. Na Bronze, manter o envelope JSON do CDC e acrescentar tópico, partição, offset e timestamp Kafka para rastreamento.

O caminho atual Kafka → Bronze usa o conector Confluent S3 10.6.11, com flush.size=1 e errors.tolerance=none. Eventos de exclusão do CDC são preservados; apenas os registros Kafka com valor nulo, chamados tombstones, são ignorados.

Silver e Gold usam Parquet e snapshots com identificadores e checksums. A referência LATEST só avança após validação; a Gold referencia o snapshot Silver utilizado. Dados brutos permanecem disponíveis para reconstrução.

## Alternativas consideradas

| Alternativa | Vantagens | Desvantagens |
|---|---|---|
| MinIO com API S3 | Separa armazenamento e processamento; oferece versionamento e integração S3. | Requer operação, política de backup e mecanismo externo de consulta. |
| Histórico em JSONB | Permite consultas SQL e aproveita o PostgreSQL existente. | Retenção e consultas disputam disco e processamento com o banco transacional. |
| Arquivos em volume | Solução inicial simples, com poucas dependências. | API, versionamento, acesso concorrente e organização precisam de controles adicionais. |

## Consequências

Ganhamos histórico auditável e reconstrução das camadas derivadas. Há custo de armazenamento para versões, eventos e snapshots. Um objeto por evento facilita a demonstração, mas gera muitos arquivos pequenos; produção precisará avaliar escrita em lotes e rotação por tempo ou tamanho.

A entrega pode envolver repetição de leitura. A identidade tópico, partição e offset permite conciliar eventos e deduplicar na Silver. A Bronze deve conservar os dados recebidos, inclusive alterações e exclusões, sem ser substituída pelas tabelas analíticas.

## Durabilidade e limites atuais

Kafka retém eventos durante uma indisponibilidade do MinIO; a retomada depende dos eventos e offsets preservados. O versionamento ajuda a recuperar sobrescritas, mas não substitui backup externo nem protege sozinho contra perda do host.

Para produção, propomos armazenamento distribuído ou replicação, cópia fora do host, restauração testada e monitoramento do atraso e do espaço livre. Não executar docker compose down -v nem apagar buckets para reiniciar uma demonstração que deve preservar o histórico.

## Escalabilidade

O aumento para 20 vezes a demanda exige medir bytes por evento, quantidade de objetos e tempo de escrita. A expansão deve considerar capacidade do armazenamento, paralelismo por partição e agregação de objetos pequenos. Não realizamos teste de carga; a configuração local não comprova capacidade para 300 mil pedidos por dia.

## Custos e licenciamento

MinIO é disponibilizado sob AGPLv3. O conector Confluent S3 usa Confluent Community License, que é source available e não é licença open source aprovada pela OSI. Portanto, o caminho atual tem uma pendência frente à exigência de usar apenas código aberto. Propomos substituir o sink por alternativa de código aberto, preservando metadados e confirmando offsets após a persistência. Essa substituição ainda não foi implementada. A aceitação deste ADR cobre o armazenamento; não atesta conformidade integral da stack.

## Implementação e evidências

Implementação: conectores/connector-minio-bronze.json, docker-compose.yml, docs/SILVER.md e docs/GOLD.md. A auditoria local de 21/10/2026 conciliou 52 eventos na Bronze. Silver e Gold têm validações próprias; seus resultados não substituem a conferência do histórico bruto.

## Referências

- [Licença do MinIO](https://github.com/minio/minio/blob/master/LICENSE)
- [Licença do conector S3](https://github.com/confluentinc/kafka-connect-storage-cloud/blob/master/LICENSE)
- [Esclarecimento oficial sobre CCL](https://www.confluent.io/confluent-community-license-faq/)
