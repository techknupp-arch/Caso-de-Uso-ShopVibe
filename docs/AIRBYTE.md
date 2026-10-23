# Airbyte no ShopVibe

O caminho de Bling e Melhor Envio segue o diagrama: API JSON -> Airbyte -> MinIO Bronze. Airflow agenda as coletas a cada 4 horas UTC; Silver e Gold executam em sequencia diariamente as 02:00 UTC. O CDC PostgreSQL -> Kafka -> MinIO nao depende do Airbyte.

ClearSale recebe pedidos pelo fluxo de eventos no desenho, sem uma DAG Airflow. Esse consumidor de negocio ainda nao esta implementado; seu JSON atual serve somente para testar a modelagem Silver. A API de pagamento tambem nao possui DAG propria.

## Iniciar

Na pasta do Compose, com Docker Desktop ativo:

```powershell
.\start-shopvibe.ps1
```

O script fixa abctl 0.30.4 e Airbyte/chart 1.8.5, verifica o pacote baixado, preserva volumes e configura recursos pela API publica. Instalar pode levar ate 30 minutos. O abctl cria o cluster local `airbyte-abctl`; nao habilite nem resete o Kubernetes do Docker Desktop para este projeto. A instalacao completa precisa de mais memoria que o projeto sem Airbyte; recomenda-se reservar 12 a 16 GB para todos os componentes. Com 8 GB, o modo de poucos recursos pode ser lento ou insuficiente dependendo dos outros containers.

`airbyte/values.json` e um arquivo Helm valido que limita as JVMs do laboratorio, aumenta a tolerancia dos healthchecks e mantem o Connector Builder habilitado. O instalador reaproveita a plataforma existente e aplica mudancas desse perfil com rollouts sequenciais, sem reinstalar banco/ingresso a cada execucao. O manifesto usa CDK 4.3.0, compativel com a plataforma fixada.

O Compose tambem limita os heaps de Kafka e Kafka Connect a 512 MB para este laboratorio. Isso reduz os picos de inicializacao ao executar a stack junto com o Airbyte; a retencao em disco e os volumes nao sao alterados.

Arquivos locais `.tools/` (binarios), `.runtime/` (credenciais, IDs e logs) e `.env` nao entram no Git. Mantenha `.env.example`, os manifestos em `airbyte/` e este instalador na entrega. Quem baixar o projeto nao precisa montar um venv para os containers.

Para consultar credenciais de acesso:

```powershell
Get-Content .\.runtime\airbyte-login.txt
```

Abra http://localhost:8001. O email inicial e `admin@shopvibe.com`, e a senha e gerada automaticamente. Credenciais de automacao sao lidas do segredo criado pelo abctl, sem precisar copiar valores manualmente. O arquivo de login e local e nao deve ser compartilhado no Git. A configuracao cria:

| Recurso | Configuracao |
|---|---|
| Fonte ShopVibe bling | API simulada `/bling/inventory` |
| Fonte ShopVibe melhor_envio | API simulada `/melhor-envio/shipments` |
| Definicao declarativa compartilhada | `airbyte/source-services.json`, stream `exports` |
| Destino ShopVibe Bronze MinIO | S3 compativel, bucket `bronze`, pasta `airbyte`, JSONL sem compressao |
| Conexoes | `bling -> Bronze (append)` e `melhor_envio -> Bronze (append)` |
| Modo | Full refresh / Append, agenda manual dentro do Airbyte |

`host.docker.internal:8002` e o endereco das APIs visto pelos pods Airbyte; o MinIO fica em `host.docker.internal:9000`. No navegador, use `localhost` nas mesmas portas. O script foi preparado para Docker Desktop no Windows.

O bootstrap corrige automaticamente o destino S3 1.9.0/1.9.1 para 1.9.2, que inclui a correcao de acesso por caminho para MinIO. Versoes posteriores escolhidas pela interface sao preservadas. A configuracao local de automacao permite leitura ao grupo compartilhado dos containers, para que o Airflow consiga acessa-la sem expor credenciais aos demais usuarios.

## Coleta com pouco codigo

No Airbyte, abra Sources e selecione a fonte. Endereco e endpoint sao parametros, nao codigo de extracao. O manifesto e JSON valido como YAML, importavel no Connector Builder. Modificacoes na interface nao sao sobrescritas pela rotina de inicializacao. O modo append e obrigatorio e verificado pelo bootstrap.

As APIs locais publicam o documento de simulacao completo em `data[0]`, incluindo `body`, `references`, `extracted_at` e `simulated=true`. Isso preserva o contrato ja usado pela Silver. Elas nao reproduzem autenticacao, paginacao ou todos os endpoints dos fornecedores reais.

Para usar Bling/Melhor Envio reais, configure ou crie no Builder os respectivos endpoints, OAuth, paginacao e estado incremental conforme as APIs oficiais. O manifesto atual e de demonstracao; trocar somente a URL nao o torna um conector pronto para producao. O contrato de entrada da Silver tambem precisa ser adaptado para a resposta real.

## Teste completo

1. Execute `start-shopvibe.ps1` e aguarde sucesso.
2. Abra as duas conexoes no Airbyte e confira o historico dos jobs. Cada exportacao e um registro Airbyte; `body` contem os varios saldos ou envios.
3. No MinIO http://localhost:9002, veja o bucket `bronze`, pasta `airbyte`. Os JSONL contem `_airbyte_data` e metadados do Airbyte.
4. No Airflow http://localhost:8080, dispare `shopvibe_servicos_4h`. As tasks devem terminar em success: `airbyte_bling`, `airbyte_melhor_envio`, `verify_bronze_airbyte`.
5. Dispare `shopvibe_silver_diaria`. Confira `build_silver`, `validate_silver`, `build_gold` e `validate_gold`, depois `silver/LATEST.json`, `gold/LATEST.json` e seus manifestos no MinIO.

Para simular uma nova coleta, altere um saldo em `samples/services/bling/inventory.json` e avance `extracted_at`. Dispare a DAG de servicos, depois a Silver. A Bronze deve conservar os arquivos anteriores e receber um novo; a Silver deve refletir a observacao mais recente. Repetir o mesmo exemplo conserva as coletas brutas e deduplica eventos iguais na Silver.

Para validar sem disparar uma nova coleta:

```powershell
docker compose exec airflow python -c "from pipelines.airbyte import verify_bronze; print(verify_bronze())"
docker compose exec airflow python -m pipelines.cli verify
```

Para sincronizar manualmente fora da agenda:

```powershell
docker compose run --rm --no-deps airbyte-init python airbyte/bootstrap.py sync
```

## Persistencia e diagnostico

O Airflow renova tokens e espera o resultado real do job; falha de sincronizacao faz a task falhar. Retentativas reaproveitam jobs ainda ativos. A verificacao da Bronze exige os conteudos atuais das duas fontes e nao considera apenas a existencia de um arquivo antigo. A leitura Silver aceita JSONL nativo, inclusive gzip, e coloca entradas invalidas em quarentena preservando LATEST.

Para logs da instalacao, consulte `.runtime/airbyte-install.log`. Para status da plataforma:

```powershell
& .\.tools\airbyte\abctl-v0.30.4-windows-amd64\abctl.exe local status
```

`docker compose stop` para os componentes do Compose; o Airbyte e separado. No Docker Desktop, pare/inicie o container `airbyte-abctl-control-plane` para pausar/retomar a plataforma existente. Nao remova volumes, nao use reset do Docker nem `abctl local uninstall --persisted` para resolver uma falha de inicializacao. O versionamento do MinIO e o modo append sao mantidos.

Referencias oficiais: [instalacao Airbyte](https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart), [abctl](https://docs.airbyte.com/platform/deploying-airbyte/abctl), [Connector Builder](https://docs.airbyte.com/platform/connector-development/config-based/understanding-the-yaml-file/reference), [destino S3/MinIO](https://docs.airbyte.com/integrations/destinations/s3), [API publica](https://reference.airbyte.com/reference/createconnection).
