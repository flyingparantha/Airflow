#   sh pull_github_repository_airflow.sh Git_Repo master Dag_ID

from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime
from airflow.models import Variable

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Team',
    'depends_on_past': False,
    'start_date': '2020-10-23',
    'email': ['email@gmail.com'],
    'email_on_failure': True,  
    'email_on_retry': False,    
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

#Instantiate a dag
dag = DAG(
    dag_id = 'DAG_ID', 
    default_args=default_args,
    description='Example of Airflow DAG Operators',
    catchup=True, 
    schedule_interval="0 0 * * 5",
)

#Declare variables
#dag home folder location
dag_home = Variable.get("DAG_ID_home")
#connection for hiveoperator
beeline_conn = Variable.get("DAG_ID_beeline")
#connection for hiverserver2hook
hive_conn = Variable.get("DAG_ID_hivehook")
#connection for sshoperator
ssh_conn = Variable.get("DAG_ID_ssh")

#this is a default variable with the run date
run_date = "{{ ds }}" 

#To select the proper queue for queries and spark commands, use below variable
bdpaas_queue_name = Variable.get('bdpaas_queue_name')

##########################################################################
#SSH Operator, profile and environments

#get set_profile variable
#set profile is needed to run commands like beeline and spark-submit
set_profile = Variable.get("set_profile")

#You can provide a direct command
import_tensor = "python -c 'import pandas'"

#get environment variable
envpython = Variable.get("envPyspark37")

#Example using direct command
import_tensor_with_env = set_profile + envpython + import_tensor

ssh_task1 = SSHOperator(
    ssh_conn_id=ssh_conn,
    task_id='ssh_task1', 
    command = import_tensor_with_env,
    dag=dag,
)

#You can export variables to use in the shell script
export_var = "export database='test';export test_date='2021-04-12';"

#call shell file to execute exporting variables
call_shell_file = set_profile + export_var + "sh " + dag_home + "/dags/DAG_ID/scripts/ssh.sh;"


ssh_task2 = SSHOperator(
    ssh_conn_id=ssh_conn,
    task_id='ssh_task2', 
    command = call_shell_file,
    dag=dag,
)

#create spark command
spark_command = "spark-submit \
--master yarn \
--deploy-mode cluster \
--queue {bdpaas_queue_name} \
--num-executors 1 \
--driver-memory 1g \
--conf spark.driver.maxResultSize=1g \
--conf spark.rpc.message.maxSize=1024 \
--executor-memory 1g \
--files /mapr/datalake/other/aes_ucee_bd_pr_2/spark/conf/hive-site.xml \
{dag_home}/dags/aes_airflow_examples/scripts/spark.py;".format(
    dag_home=dag_home,
    bdpaas_queue_name=bdpaas_queue_name
    )

#build spark command into a variable
call_spark_command = set_profile + envpython + spark_command

spark_task = SSHOperator(
    ssh_conn_id=ssh_conn,
    task_id='spark_task', 
    command = call_spark_command,
    dag=dag,
)

##########################################################################
#HiveOperator

test_date = '2016-01-01'
db = "Database_Name"

#Example of direct hql query
direct_query= """
USE {db};
set mapred.job.queue.name={bdpaas_queue_name}; 

select * from lob where upd_tstmp > "{test_date}";
""".format(
    db=db,
    bdpaas_queue_name=bdpaas_queue_name,
    test_date=test_date
    )

hql_task_direct_query = HiveOperator(
    task_id="hql_task_direct_query",
    hql=direct_query,
    hive_cli_conn_id=beeline_conn,
    dag=dag
)

#Example of using an hql file
query_file = open(dag_home + '/dags/DAG_ID/hql/hive_operator.hql', 'r').read()

hql_task_query_file = HiveOperator(
    task_id="hql_task_query_file",
    hql=query_file,
    params={ 'db' : db , 'date_p' : test_date, 'bdpaas_queue_name' : bdpaas_queue_name },
    hive_cli_conn_id=beeline_conn,
    dag=dag
)

##########################################################################
#Short Circuit Operator

#Python function
def check_results_true(**kwargs):
    hh = HiveServer2Hook(hiveserver2_conn_id=hive_conn,**kwargs)

    #Example of running multiple queries in one connection
    max_upd_tstmp = ['set mapred.job.queue.name={bdpaas_queue_name}'.format(bdpaas_queue_name=bdpaas_queue_name),'select max(upd_tstmp) from raw_ucee_nr.lob']

    #results from .get_records is a list inside a list, or multi dimensional array
    records = hh.get_records(max_upd_tstmp)
    max_upd_tstmp_res = records[0][0]

    print("Result of first query: " + str(max_upd_tstmp_res))
    print("Validating results...")

    #The ShortCircuitOperator receives this result and if False, all downstream tasks will get skipped.
    #If True, rest of the tasks will continue as normal

    to_check = "2000-01-01"
    to_check_date = datetime.strptime(to_check, '%Y-%m-%d')

    if (max_upd_tstmp_res > to_check_date):
        return True
    else:
        return False
    
#ShortCircuitOperator
evaluate_results_true = ShortCircuitOperator(
    task_id='evaluate_results_true',
    provide_context=True,
    python_callable=check_results_true,
    dag=dag)

#Python function
def check_results_false(**kwargs):
    hh = HiveServer2Hook(hiveserver2_conn_id=hive_conn,**kwargs)

    #Example of running multiple queries in one connection
    max_upd_tstmp = ['set mapred.job.queue.name={bdpaas_queue_name}'.format(bdpaas_queue_name=bdpaas_queue_name),'select max(updated_tstmp) from Database.table_name']

    #results from .get_records is a list inside a list, or multi dimensional array
    records = hh.get_records(max_upd_tstmp)
    max_upd_tstmp_res = records[0][0]

    print("Result of first query: " + str(max_upd_tstmp_res))
    print("Validating results...")

    #The ShortCircuitOperator receives this result and if False, all downstream tasks will get skipped.
    #If True, rest of the tasks will continue as normal

    to_check = "2022-01-01"
    to_check_date = datetime.strptime(to_check, '%Y-%m-%d')

    if (max_upd_tstmp_res > to_check_date):
        return True
    else:
        return False
    
#ShortCircuitOperator
evaluate_results_false = ShortCircuitOperator(
    task_id='evaluate_results_false',
    provide_context=True,
    python_callable=check_results_false,
    dag=dag)

dummy = SSHOperator(
    ssh_conn_id=ssh_conn,
    task_id='dummy', 
    command = "echo test",
    dag=dag,
)


##########################################################################
#Export file using HiveServer2Hook
#Python function
def export_file(**kwargs):
    hh = HiveServer2Hook(hiveserver2_conn_id=hive_conn,**kwargs)    

    #Example of running multiple queries in one connection
    export_table = ['set mapred.job.queue.name={bdpaas_queue_name}'.format(bdpaas_queue_name=bdpaas_queue_name),'set hive.resultset.use.unique.column.names=false','select * from Database.Table_name']
    hh.to_csv(export_table, dag_home + '/dags/DAG_ID/files/airflow_testing.csv', schema='default', delimiter=',', lineterminator='\r\n', output_header=True, fetch_size=1000, hive_conf=None)
   
#PythonOperator
export_the_file = PythonOperator(
    task_id='export_the_file',
    provide_context=True,
    python_callable=export_file,
    dag=dag)

##########################################################################
#Replicating table to Tenant 3

#get replicate_ten3
replicate_ten3 = Variable.get("replicate_ten3")

#executing command directly
direct_command = set_profile + replicate_ten3 + 'true test.airflow_testing;'

ssh_rep_ten3 = SSHOperator(
    ssh_conn_id=ssh_conn,
    task_id='ssh_rep_ten3',
    command=direct_command,
    dag=dag,
    )

##########################################################################
#Sending emails
from airflow.utils.email import send_email
def send_final_email():
    email_list = ['email@google.com']
    with open(dag_home + "/dags/aes_airflow_examples/template.html") as f:
        content = f.read()
    send_email(
        to=email_list,
        subject='Email Send Example',
        html_content=content,
        files=[dag_home + "/dags/DAG_ID/template.html"],
    )
email_send = PythonOperator(
    task_id='email_send',
    python_callable=send_final_email,
    provide_context=False,
    dag=dag,
)

###########################################################################
#Workflow

[hql_task_query_file,hql_task_direct_query] >> evaluate_results_true >> export_the_file >> ssh_rep_ten3 >> email_send
[hql_task_query_file,hql_task_direct_query] >> evaluate_results_false >> dummy
ssh_task1 >> ssh_task2 >> spark_task
