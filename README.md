# Forex Hybrid ETL

Pipeline de engenharia de dados para cotações Forex da Tiingo. O projeto combina
ingestão em tempo real com uma carga histórica diária, usando Kafka, PostgreSQL,
Python e Apache Airflow.

> **Estado:** a ingestão histórica, a execução diária no Airflow,
> a análise SQL e a geração de um relatório CSV foram executadas e verificadas
> localmente. Consulte [Acompanhamento do projeto](#acompanhamento-do-projeto)
> para o estado atual e as tarefas ainda pendentes antes da publicação no GitHub.

## Conteúdo

- [Arquitetura](#arquitetura)
- [O que já está implementado](#o-que-já-está-implementado)
- [Tecnologias e portas](#tecnologias-e-portas)
- [Pré-requisitos](#pré-requisitos)
- [Configuração local](#configuração-local)
- [Base de dados e migrações](#base-de-dados-e-migrações)
- [Executar o pipeline em tempo real](#executar-o-pipeline-em-tempo-real)
- [Airflow e histórico diário](#airflow-e-histórico-diário)
- [Relatórios](#relatórios)
- [Testes](#testes)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Segurança e publicação no GitHub](#segurança-e-publicação-no-github)
- [Acompanhamento do projeto](#acompanhamento-do-projeto)

## Arquitetura

```text
Tempo real:
Tiingo Forex WebSocket -> produtor Python -> Kafka -> consumidor Python
                                                        |
                                                        v
                                               PostgreSQL / fx_quotes

Histórico diário:
Tiingo Historical API -> Airflow: extract -> prepare -> validate -> load
                                                               |
                                                               v
                                                   PostgreSQL / fx_daily_bars
                                                               |
                                                               v
                                                   relatório CSV / data/reports
```

O PostgreSQL contém duas tabelas separadas:

- `fx_quotes`: ticks recebidos pelo fluxo em tempo real.
- `fx_daily_bars`: velas históricas diárias OHLC.

O Airflow usa uma base de metadados chamada `airflow`; os dados Forex ficam na
base `forex`. Não confundir as duas bases ao ligar o pgAdmin.

## O que já está implementado

- Produtor WebSocket Tiingo que normaliza cotações e publica no tópico Kafka
  `forex-quotes`.
- Consumidor Kafka com validação, persistência idempotente em `fx_quotes` e
  confirmação do offset somente depois do processamento.
- Extração de velas históricas Tiingo, preservação da resposta JSON bruta e
  preparação/validação de CSV de staging.
- Carga histórica com UPSERT em `fx_daily_bars`, pela chave
  `(source, symbol, event_time)`.
- Airflow 3.3.2 com `LocalExecutor` e a DAG tutorial `airflow_primeira_dag`.
- DAG `tiingo_fx_historical`: extração, preparação, validação, carga e geração
  do resumo CSV. Está configurada para executar diariamente às 00:00 UTC, sem
  catchup. A janela automática cobre os três dias completos anteriores; numa
  execução manual, também é possível indicar ambas as datas.
- View PostgreSQL `v_fx_daily_analysis` e migração SQL versionada para a criar.
- Relatório CSV de resumo com contagem de velas e variações percentuais.
- Testes unitários locais: **38 aprovados** na última verificação registada.

Tiingo pode não devolver uma vela para todos os dias do intervalo pedido. Fins
de semana e disponibilidade da fonte explicam intervalos com menos registos;
validar a contagem real é parte do fluxo.

## Tecnologias e portas

| Componente | Versão/serviço | Endereço local |
|---|---|---|
| PostgreSQL | `postgres:16-alpine` | `127.0.0.1:5433` |
| Kafka | `confluentinc/cp-kafka:7.6.1` | `localhost:9092` |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | `http://localhost:8080` |
| Airflow | `apache/airflow:3.3.2` | `http://localhost:8081` |
| Python | ambiente local do projeto | 3.11 recomendado |

O pgAdmin é um cliente desktop: para esta máquina, ligue-se ao host
`127.0.0.1`, porta `5433`, base `forex`, utilizador definido por
`POSTGRES_USER` e palavra-passe definida localmente por `POSTGRES_PASSWORD`.
Dentro da rede Docker, os containers usam o host `postgres` e a porta `5432`.

## Pré-requisitos

- Windows com Docker Desktop e Docker Compose disponíveis no PowerShell.
- Python 3.11 para executar os testes e os processos de tempo real no host.
- Uma conta/token Tiingo com acesso aos produtos Forex que pretende usar.
- Opcional: pgAdmin ou outro cliente PostgreSQL.

## Configuração local

1. Obtenha uma cópia do repositório e abra o PowerShell na pasta do projeto.
2. Crie o ficheiro local de variáveis de ambiente:

   ```powershell
   Copy-Item .env.example .env
   notepad .env
   ```

3. Preencha `TIINGO_API_TOKEN` e crie valores privados para `POSTGRES_PASSWORD`,
   `AIRFLOW_FERNET_KEY` e `AIRFLOW_JWT_SECRET`. Defina também uma palavra-passe
   administrativa em `AIRFLOW_ADMIN_PASSWORD`. Não partilhe nem faça commit do
   `.env`.

   Para gerar os segredos Airflow com a imagem já usada pelo projeto:

   ```powershell
   docker run --rm apache/airflow:3.3.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   docker run --rm apache/airflow:3.3.2 python -c "import secrets; print(secrets.token_urlsafe(32))"
   ```

   O primeiro comando gera `AIRFLOW_FERNET_KEY`; o segundo gera
   `AIRFLOW_JWT_SECRET`. Use uma palavra-passe forte, privada e exclusiva para
   PostgreSQL e outra para o utilizador administrador.

4. Crie e ative o ambiente Python:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

5. Valide o Compose sem imprimir a configuração expandida:

   ```powershell
   docker compose config --quiet
   ```

6. Inicie os serviços Docker:

   ```powershell
   docker compose up airflow-init
   docker compose up -d
   docker compose ps
   ```

   Aguarde o PostgreSQL ficar `healthy` e confirme que os serviços Airflow estão
   ativos. A interface do Airflow fica em `http://localhost:8081`. O utilizador
   e a palavra-passe são os valores locais de `AIRFLOW_ADMIN_USERNAME` e
   `AIRFLOW_ADMIN_PASSWORD`.

O exemplo de ambiente contém nomes de variáveis e valores vazios para segredos;
é necessário preenchê-los no `.env` local antes de iniciar os serviços.

## Base de dados e migrações

Na primeira configuração, crie/aplique os scripts na base `forex`, por esta
ordem:

1. `sql/schema.sql` — tabela de cotações em tempo real `fx_quotes`.
2. `sql/migrations/001_add_quote_price_fields.sql` — colunas tipadas das
   cotações em tempo real.
3. `sql/migrations/002_create_fx_daily_bars.sql` — tabela histórica.
4. `sql/migrations/003_create_fx_daily_analysis_view.sql` — view analítica.

Os scripts SQL são versionados, mas ainda não existe um mecanismo automático de
controlo/aplicação de migrações; aplique-os conscientemente no Query Tool do
pgAdmin, ligado à base `forex`. A migração da view usa `CREATE OR REPLACE VIEW` e
pode ser executada novamente para atualizar a definição.

## Executar o pipeline em tempo real

Com Kafka e PostgreSQL ativos, abra **dois terminais PowerShell** na raiz do
projeto, ative o ambiente `.venv` em ambos e execute:

Terminal 1 — consumidor:

```powershell
python -m src.consumers.forex_consumer
```

Terminal 2 — produtor:

```powershell
python -m src.producers.tiingo_ws_producer
```

O produtor lê `TIINGO_API_TOKEN` e `FOREX_SYMBOL` do ambiente. O consumidor
valida as mensagens Kafka e grava as cotações em `fx_quotes`. Para parar cada
processo, use `Ctrl+C`.

## Airflow e histórico diário

Abra `http://localhost:8081`, entre com as credenciais administrativas locais e
procure `tiingo_fx_historical`. Na primeira utilização, confirme que a DAG está
ativa. Pode acionar uma execução pela interface ou aguardar o horário diário.

Parâmetros de execução manual:

- `symbol`: símbolo de seis letras, predefinido como `EURUSD`.
- `start_date` e `end_date`: preencha **ambos** no formato `AAAA-MM-DD` para
  escolher um intervalo inclusivo. Se ambos ficarem vazios, a DAG usa a janela
  automática de três dias completos anteriores.

As tarefas são executadas nesta ordem:

1. `extract`: consulta a API histórica Tiingo e guarda JSON em `data/raw`.
2. `prepare`: converte a resposta em CSV canónico em `data/staging`.
3. `validate`: verifica estrutura, datas, duplicados e coerência OHLC.
4. `load`: carrega com UPSERT em `fx_daily_bars`.
5. `generate_report`: consulta o PostgreSQL e escreve o relatório CSV em
   `data/reports`; só corre depois de a carga ter sucesso.

Os ficheiros usam nomes com timestamp para não sobrescrever execuções
anteriores. As pastas `data/raw`, `data/staging` e `data/reports` não devem ser
publicadas com dados reais.

## Relatórios

Os relatórios criados pela DAG ficam em `data/reports`. Cada CSV contém o
símbolo, as datas pedidas e disponíveis, o número de velas, o número de
comparações com fecho anterior e as variações média, mínima e máxima em
percentagem.

Também é possível consultar os dados no pgAdmin. Exemplo de leitura da view:

```sql
SELECT
    symbol,
    data_utc,
    open,
    high,
    low,
    close,
    previous_close,
    variacao_desde_fecho_anterior_pct
FROM v_fx_daily_analysis
WHERE source = 'tiingo'
  AND symbol = 'EURUSD'
ORDER BY event_time;
```

## Testes

Execute os testes unitários a partir da raiz, com o ambiente virtual ativo:

```powershell
python -m unittest discover -s tests -v
```

Os testes cobrem normalização, validação do consumidor/produtor, extração
histórica, staging, carga e geração do relatório. A última execução registada
teve **38 testes aprovados**. Ainda falta configurar execução automática destes
testes no GitHub Actions.

## Estrutura do projeto

```text
airflow/dags/       DAG tutorial e DAG histórica
data/raw/           respostas JSON da API (dados locais)
data/staging/       CSVs intermédios (dados locais)
data/reports/       relatórios CSV (dados locais)
scripts/            scripts auxiliares, quando aplicável
sql/                schema e migrações PostgreSQL
src/                produtor, consumidor, extractores, staging, loaders e reports
tests/              testes unitários
docker-compose.yml  Kafka, PostgreSQL e Airflow
.env.example        modelo sem segredos para configuração local
requirements.txt    dependências Python da aplicação
requirements-lock.txt
                    lock atual a rever antes de ser usado como instalação
```

## Segurança e publicação no GitHub

- Nunca publique `.env`, tokens Tiingo, passwords, chaves Airflow, logs ou dados
  recebidos da API.
- `.gitignore` exclui `.env`, logs e o conteúdo de `data/`, mantendo apenas
  ficheiros `.gitkeep` para as pastas de dados.
- Antes do primeiro push, confirme `git status`, reveja cada ficheiro staged e
  execute um scanner de segredos. Se uma credencial for exposta, revogue-a e
  gere uma nova; apagar o valor do último commit não remove o segredo do
  histórico Git.
- Os serviços locais Kafka estão configurados sem autenticação/TLS. Não exponha
  estas portas diretamente à Internet.
- A configuração foi validada para aprendizagem e execução local, não é uma
  implantação de produção.

## Acompanhamento do projeto

### Concluído e verificado

- [x] Ambiente Docker com Kafka, PostgreSQL e Airflow local.
- [x] Fluxo em tempo real Tiingo → Kafka → PostgreSQL (`fx_quotes`).
- [x] Extração histórica, staging, validação e UPSERT (`fx_daily_bars`).
- [x] DAG manual e agendamento diário às 00:00 UTC, com janela móvel.
- [x] Consulta analítica e view `v_fx_daily_analysis`.
- [x] Relatório CSV dependente da carga e guardado em `data/reports`.
- [x] Testes unitários locais: 38 aprovados na última verificação.
- [x] README inicial e lista de preparação para GitHub.

### Em falta antes de publicar

- [ ] Fazer a revisão final dos ficheiros staged e uma análise de segredos;
  confirmar que nenhum segredo ou dado real entra no Git. `.env.example` foi
  revisto nesta etapa e não contém uma palavra-passe de exemplo.
- [ ] Rever `requirements-lock.txt`: o conteúdo atual inclui pacotes de uma
  instalação ampla e não é ainda um lock mínimo/reproduzível do projeto.
- [ ] Executar novamente todos os testes num ambiente limpo e validar o Compose
  usando apenas o `.env.example` preenchido localmente.
- [ ] Adicionar GitHub Actions para executar testes e validações básicas em
  pull requests.
- [ ] Escolher e adicionar uma licença, se o repositório for público.
- [ ] Criar/configurar o repositório remoto GitHub, escolher o nome/descrição e
  definir a branch principal; rever o estado Git antes do primeiro commit/push.
- [ ] Documentar política de retenção dos dados e estratégia de backup se o
  ambiente passar a ser usado continuamente.

### Estado do Git local

Na atualização deste README, a branch local era `master`, não havia remote
GitHub configurado e os ficheiros do projeto ainda apareciam como não rastreados
no Git. Não foi criado commit nem feito push. Antes do primeiro commit, reveja
o conteúdo que será adicionado e confirme novamente `git status`.

### Próximos objetivos técnicos

- [ ] Automatizar o controlo e a aplicação de migrações PostgreSQL.
- [ ] Adicionar testes de integração da DAG, PostgreSQL e Kafka em ambiente
  descartável.
- [ ] Configurar alertas para falhas e monitorização das execuções agendadas.
- [ ] Rever retries e alertas para `prepare`, `validate` e `generate_report`,
  considerando quais falhas são realmente temporárias.
- [ ] Avaliar autenticação e TLS antes de qualquer implantação fora da máquina
  local.
