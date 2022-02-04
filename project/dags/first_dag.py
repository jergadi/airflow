try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All Dag modules are ok......")
except Exception as e:
    print("Error {} ".format(e))

def first_function_execute(**context):
    print("First Function Execute    ")
    context['ti'].xcom_push(key="mykey", value="first_function_execute says hello ")

def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    print("I am in second_function_execute got value :{} from Function 1".format(instanceFAILEME))

def retry_callback(context):
    last_task = context.get('task_instance')
    retry_number = last_task.try_number
    task_name = last_task.task_id
    log_link = f"<{last_task.log_url}|{task_name}>"
    error_message = context.get('exception') or context.get('reason')

    execution_date = context.get('execution_date')
    title = f':red_circle: Retrying task {task_name}, retries left {retry_number}'
    msg_parts = {
        'DAG': "first_dag",
        'Task name': task_name,
        'Execution date': execution_date,
        'Log': log_link,
        'Error': error_message
    }
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()
    print(msg)

def failure_callback(context):
    last_task = context.get('task_instance')
    task_name = last_task.task_id
    log_link = f"<{last_task.log_url}|{task_name}>"
    error_message = context.get('exception') or context.get('reason')

    execution_date = context.get('execution_date')
    title = f':red_circle: Failure of task {task_name}'
    msg_parts = {
        'DAG': "first_dag",
        'Task name': task_name,
        'Execution date': execution_date,
        'Log': log_link,
        'Error': error_message
    }
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()
    print(msg)


with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2020,11,1),
        "on_retry_callback": retry_callback,
        "on_failure_callback": failure_callback

    },
    catchup=False
) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name": "Jerome Gaditano"}
    )
    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True
    )

first_function_execute >> second_function_execute