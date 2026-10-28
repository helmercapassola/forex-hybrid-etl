import unittest
from datetime import timedelta

import pendulum
from airflow.dag_processing.dagbag import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState


class TestTiingoFxHistoricalRecovery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dag_bag = DagBag(
            dag_folder="/opt/airflow/dags",
        )
        if dag_bag.import_errors:
            raise AssertionError(f"DAG import errors: {dag_bag.import_errors}")

        cls.dag = dag_bag.get_dag("tiingo_fx_historical")
        if cls.dag is None:
            raise AssertionError("DAG tiingo_fx_historical was not loaded.")

    def test_transient_load_failure_retries_and_dag_recovers(self):
        calls = {
            "extract": 0,
            "prepare": 0,
            "validate": 0,
            "load": 0,
            "generate_report": 0,
        }

        def succeed(task_id, result):
            def callable_for_task(*args, **kwargs):
                calls[task_id] += 1
                return result

            return callable_for_task

        def fail_once_then_load(*args, **kwargs):
            calls["load"] += 1
            if calls["load"] == 1:
                raise RuntimeError("Simulated transient PostgreSQL failure")
            return 1

        replacements = {
            "extract": succeed("extract", "/tmp/raw.json"),
            "prepare": succeed("prepare", "/tmp/staging.csv"),
            "validate": succeed("validate", 1),
            "load": fail_once_then_load,
            "generate_report": succeed(
                "generate_report",
                "/tmp/report.csv",
            ),
        }

        for task_id, callable_for_task in replacements.items():
            task = self.dag.get_task(task_id)
            task.python_callable = callable_for_task
            task.retry_delay = timedelta(0)

        dag_run = self.dag.test(
            logical_date=pendulum.datetime(2026, 10, 28, tz="UTC"),
            run_conf={
                "symbol": "EURUSD",
                "start_date": "2026-10-27",
                "end_date": "2026-10-27",
            },
        )

        self.assertEqual(dag_run.state, DagRunState.SUCCESS)
        self.assertEqual(calls, {
            "extract": 1,
            "prepare": 1,
            "validate": 1,
            "load": 2,
            "generate_report": 1,
        })
        for task in self.dag.tasks:
            task_instance = dag_run.get_task_instance(task.task_id)
            self.assertEqual(task_instance.state, TaskInstanceState.SUCCESS)

    def test_extraction_and_load_have_two_retries(self):
        for task_id in ("extract", "load"):
            with self.subTest(task_id=task_id):
                task = self.dag.get_task(task_id)
                self.assertEqual(task.retries, 2)
                self.assertEqual(task.retry_delay, timedelta(minutes=1))


if __name__ == "__main__":
    unittest.main()
