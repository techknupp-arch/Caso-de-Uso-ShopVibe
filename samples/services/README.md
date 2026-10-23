# Exportacoes simuladas

Os JSONs destas pastas sao dados ficticios para desenvolver e testar a integracao de uma API de pagamento generica, ClearSale, Bling e Melhor Envio. `shopvibe_demo` fornece clientes, produtos e pedidos relacionados, sem gravar registros ficticios no PostgreSQL operacional.

O envelope inclui metadados nossos e o corpo simplificado da resposta em `body`. `references` associa os IDs dos fornecedores aos IDs da ShopVibe. Os exemplos nao sao uma copia completa nem uma homologacao dos contratos reais de API.

Para Bling e Melhor Envio, altere `inventory.json` ou `shipments.json`, mantenha `contract_version=1` e `simulated=true`, e avance `extracted_at`. O servico `mock-services` publica esses arquivos em endpoints HTTP; o Airbyte coleta e conserva cada execucao em JSONL na Bronze, com metadados de ingestao.

Para os demais exemplos, acrescente um novo JSON ou altere o existente; a copia para Bronze usa o hash do conteudo. A Silver deduplica eventos iguais sem apagar arquivos antigos. Veja a [operacao Airbyte](../../docs/AIRBYTE.md) e a [modelagem Silver](../../docs/SILVER.md).
