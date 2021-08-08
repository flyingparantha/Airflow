# Airflow

### Links for Learning Purpose
https://airflow.apache.org/docs/stable/concepts.html#bitshift-composition

https://stackoverflow.com/questions/54579883/airflow-run-tasks-at-different-times-in-the-same-dag/54587355#54587355

In the below example, you can have 4 dags running and each dag can have 4 tasks running at the same time. Other combinations are also possible.

dag = DAG(
    dag_id='sample_dag',
    default_args=default_args,
    schedule_interval='@daily',
    -- this will allow up to 16 tasks to be run at the same time
    concurrency=16,
    -- this will allow up to 4 dags to be run at the same time
    max_active_runs=4,
)

https://stackoverflow.com/questions/51325525/wiring-top-level-dags-together
https://stackoverflow.com/questions/63565229/schedule-airflow-job-bi-weekly    ---- check this
https://stackoverflow.com/questions/57104547/how-to-define-a-dag-that-scheduler-a-monthly-job-together-with-a-daily-job
https://en.wikipedia.org/wiki/Cron#CRON_expression

