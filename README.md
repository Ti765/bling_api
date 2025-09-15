# Bling NF-e Extractor

Pequeno conjunto de scripts para consultar a API do Bling, extrair detalhes de NF-e e baixar os XMLs.

Principais arquivos
- `Aura/extrai_bling_nfe_single.py`: autentica na API do Bling, lista IDs de NF-e no período definido por variáveis de ambiente e grava os detalhes em um arquivo `JSONL` (um objeto JSON por linha). Gera também um arquivo de metadados e uma fila de retry enquanto processa notas que retornarem erros transitórios.
- `Aura/baixar_xmls_por_jsonl.py`: lê um arquivo `JSONL` gerado pelo script de extração e baixa os arquivos XML referenciados nos objetos (campos `xml` ou `linkXml`). Faz retries em passes e escreve os XMLs em uma pasta de saída.
- `Aura/requirements.txt`: dependências do projeto (instale com `pip install -r Aura/requirements.txt`).
- `Aura/.env.example`: exemplo de variáveis de ambiente (NÃO colocar segredos reais neste arquivo).

Pré-requisitos
- Python 3.8+ (recomendado)
- Instalar dependências:

```powershell
python -m pip install -r Aura\requirements.txt
```

Configuração
- Crie um arquivo `.env` baseado em `Aura/.env.example` e preencha com as credenciais e o período desejado. Não comite seu `.env` com segredos.
- Principais variáveis (resumido): `BLING_CLIENT_ID`, `BLING_CLIENT_SECRET`, `BLING_REFRESH_TOKEN` ou `BLING_AUTHORIZATION_CODE`, `NFE_INICIO`, `NFE_FIM`, `OUTPUT_DIR`.

Uso
- Extrair detalhes das notas (gera `out/<empresa>_DDMMYYYY_DDMMYYYY.jsonl`):

```powershell
python Aura\extrai_bling_nfe_single.py
```

- Baixar XMLs a partir do JSONL gerado:

```powershell
python Aura\baixar_xmls_por_jsonl.py <caminho_para_arquivo.jsonl> [pasta_saida]
# Exemplo:
python Aura\baixar_xmls_por_jsonl.py out\AURA_01012025_31012025.jsonl .\xml
```

Saídas
- `out/` (por padrão):
  - `*.jsonl` — detalhes das notas (um JSON por linha)
  - `*.meta.json` — metadados do processamento
  - `*.retry_queue.json` — fila de retry entre passes
- `xml/` (pasta de saída do downloader): arquivos `*.xml` baixados
- `tokens.json` pode ser criado se `BLING_SAVE_TOKENS=json` (ou o token pode ser salvo em `.env` se `BLING_SAVE_TOKENS=env`).

Segurança
- Nunca comite credenciais reais. Use `Aura/.env.example` como referência e mantenha seu `.env` fora do repositório.

Observações
- O projeto usa apenas `requests` como dependência externa; o restante é biblioteca padrão do Python.
- Se quiser que eu fixe versões no `requirements.txt` (pin exato) ou gere um `requirements.txt` completo do ambiente com `pip freeze`, posso fazer isso.
