# Decisões arquiteturais da ShopVibe

Autores: Daniel e Jessica Fernanda Knupp. Data dos registros: 21/10/2026.

Os três ADRs documentam as escolhas do caminho PostgreSQL → Debezium → Kafka → MinIO. Cada versão Word tem até duas páginas; o Markdown mantém o mesmo conteúdo para leitura e revisão no Git.

| Registro | Decisão | Status | Word |
|---|---|---|---|
| [ADR-001](ADR-001-captura-cdc-postgresql.md) | Captura de pedidos com CDC no PostgreSQL | Aceito | [Documento](ADR-001-captura-cdc-postgresql.docx) |
| [ADR-002](ADR-002-transporte-kafka.md) | Transporte e retenção de eventos com Kafka | Aceito | [Documento](ADR-002-transporte-kafka.docx) |
| [ADR-003](ADR-003-armazenamento-minio.md) | Histórico bruto e camadas de dados no MinIO | Aceito | [Documento](ADR-003-armazenamento-minio.docx) |

## Limites e pendências

As garantias locais são distintas das propostas para produção. Replicação, testes de falha e capacidade em Black Friday não foram demonstrados. A auditoria citada é uma evidência datada, não uma verificação contínua.

O ADR-003 registra a pendência da licença Confluent Community License do sink S3 frente ao requisito de usar apenas ferramentas de código aberto. O Airbyte opcional também exige avaliação própria de licenciamento; estes ADRs não atestam conformidade integral da stack.

## Histórico das decisões

Preservar estes registros. Para mudar uma decisão, criar um novo ADR numerado, referenciar o anterior e marcar o anterior como Substituído, apontando o sucessor. Ajustes de redação devem ficar rastreáveis pelo Git.

O estudo de caso também pede um diagrama de uma página e um vídeo de até cinco minutos. Esses materiais são entregas separadas dos ADRs.
