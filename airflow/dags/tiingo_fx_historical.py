"""DAG manual para extrair, validar e carregar histórico Forex da Tiingo."""

from datetime import date, datetime, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task


def _resolve_date_range(
    start_date_value: str | None,
    end_date_value: str | None,
    data_interval_end: datetime,
) -> tuple[date, date]:
    if bool(start_date_value) != bool(end_date_value):
        raise ValueError(
            "Preencha start_date e end_date juntos, ou deixe ambos vazios."
        )

    if start_date_value and end_date_value:
        start_date = date.fromisoformat(start_date_value)
        end_date = date.fromisoformat(end_date_value)
    else:
        # A janela baseia-se no intervalo do Airflow, não na hora de arranque.
        end_date = data_interval_end.date() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)

    if start_date > end_date:
        raise ValueError("A data inicial não pode ser posterior à data final.")

    return start_date, end_date


@dag(
    dag_id="tiingo_fx_historical",
    description="Extrai e carrega velas diárias Forex da Tiingo.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Executar diariamente à meia-noite UTC.
    schedule="0 0 * * *",
    catchup=False,
    params={
        "symbol": Param(
            "EURUSD",
            type="string",
            pattern="^[A-Za-z]{6}$",
            description="Símbolo Forex com seis letras, por exemplo EURUSD.",
        ),
        "start_date": Param(
            None,
            type=["null", "string"],
            description=(
                "Primeiro dia a extrair (AAAA-MM-DD). "
                "Se vazio juntamente com end_date, usa os últimos 3 dias completos."
            ),
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            description=(
                "Último dia a extrair (AAAA-MM-DD). "
                "Se vazio juntamente com start_date, usa os últimos 3 dias completos."
            ),
        ),
    },
    tags=["forex", "tiingo", "historico"],
)
def tiingo_fx_historical():
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract() -> str:
        # Os imports ficam dentro da tarefa para o DAG processor não precisar
        # de ler o token Tiingo apenas para descobrir as tarefas.
        from src.extractors.tiingo_fx_history import (
            fetch_historical_quotes,
            save_raw_response,
        )

        # Os parâmetros são definidos quando se inicia a DAG pela interface.
        context = get_current_context()
        params = context["params"]
        symbol = params["symbol"]
        start_date, end_date = _resolve_date_range(
            params["start_date"],
            params["end_date"],
            context["data_interval_end"],
        )

        records = fetch_historical_quotes(symbol, start_date, end_date)
        raw_file = save_raw_response(records, symbol, start_date, end_date)
        print(
            f"Extração concluída: {len(records)} registo(s) "
            f"para {symbol.upper()} entre {start_date} e {end_date}."
        )
        # XCom transporta apenas o caminho, não o conteúdo completo da resposta.
        return str(raw_file)

    @task
    def prepare(raw_file_path: str) -> str:
        from pathlib import Path

        from src.staging.tiingo_fx_history import prepare_staging_file

        staging_file = prepare_staging_file(Path(raw_file_path))
        print(f"Ficheiro de staging criado: {staging_file}")
        return str(staging_file)

    @task
    def validate(staging_file_path: str) -> int:
        from pathlib import Path

        from src.staging.tiingo_fx_history import validate_staging_file

        valid_rows = validate_staging_file(Path(staging_file_path))
        print(f"Validação concluída: {valid_rows} registo(s) válido(s).")
        return valid_rows

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def load(staging_file_path: str, valid_rows: int) -> int:
        from pathlib import Path

        from src.loaders.load_fx_history import load_staging_file

        loaded_rows = load_staging_file(Path(staging_file_path))
        if loaded_rows != valid_rows:
            raise ValueError(
                f"Esperados {valid_rows} registo(s), "
                f"mas foram carregados {loaded_rows}."
            )
        print(
            f"Carga concluída: {loaded_rows} registo(s) "
            "processado(s) em fx_daily_bars."
        )
        return loaded_rows

    @task
    def generate_report(loaded_rows: int) -> str:
        from src.reports.fx_daily_summary import create_daily_summary_report

        context = get_current_context()
        params = context["params"]
        start_date, end_date = _resolve_date_range(
            params["start_date"],
            params["end_date"],
            context["data_interval_end"],
        )
        report_path = create_daily_summary_report(
            params["symbol"],
            start_date,
            end_date,
        )
        print(
            f"Relatório criado após a carga de {loaded_rows} registo(s): "
            f"{report_path}"
        )
        return str(report_path)

    # As chamadas criam as tarefas e ligam os seus resultados:
    # extract -> prepare -> validate -> load -> generate_report.
    raw_file_path = extract()
    staging_file_path = prepare(raw_file_path)
    valid_rows = validate(staging_file_path)
    loaded_rows = load(staging_file_path, valid_rows)
    generate_report(loaded_rows)


tiingo_fx_historical()
