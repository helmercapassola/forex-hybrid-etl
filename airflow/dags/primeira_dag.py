"""DAG demonstrativa para  observar execuções no Airflow."""

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="airflow_primeira_dag",
    description="Exemplo de duas tarefas ligadas com a API TaskFlow.",
    # Data inicial da DAG; usar UTC evita ambiguidades de fuso horário.
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # None significa que esta DAG só é iniciada manualmente pela interface.
    schedule=None,
    # Não criar execuções antigas automaticamente ao ativar a DAG.
    catchup=False,
    tags=["tutorial"],
)
def primeira_dag():
    @task
    def preparar_mensagem() -> str:
        # O corpo desta função corre quando a tarefa for executada,
        # não quando o processador apenas lê este ficheiro.
        mensagem = "A primeira tarefa terminou corretamente."
        print(mensagem)
        # O valor de retorno é passado à tarefa seguinte através de XCom.
        return mensagem

    @task
    def confirmar_execucao(mensagem: str) -> None:
        # Esta tarefa só começa depois de receber o resultado da anterior.
        print(f"Mensagem recebida da tarefa anterior: {mensagem}")

    # Chamar as funções decoradas cria as tarefas e define a dependência:
    # preparar_mensagem -> confirmar_execucao.
    confirmar_execucao(preparar_mensagem())


# Registar a DAG para que o Airflow a encontre neste ficheiro.
primeira_dag()
