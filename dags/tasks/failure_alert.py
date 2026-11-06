
import logging


def failure_alert(context):
    """Log details when an Airflow task or DAG fails."""

    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    dag_id = (
        getattr(task_instance, "dag_id", None)
        or getattr(dag_run, "dag_id", "unknown")
    )

    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = getattr(dag_run, "run_id", "unknown")

    logical_date = (
        context.get("logical_date")
        or context.get("execution_date")
    )

    exception = context.get("exception")

    logging.error(
        """
        AIRFLOW FAILURE ALERT

        DAG ID: %s
        Task ID: %s
        Run ID: %s
        Logical Date: %s
        Exception: %s
        """,
        dag_id,
        task_id,
        run_id,
        logical_date,
        exception or "Not available",
    )
