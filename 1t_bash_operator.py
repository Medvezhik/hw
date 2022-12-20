from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import random
def hello():
    print('airflow')

def generate_num():
    num_1 = random.randint(1, 100)
    num_2 = random.randint(1, 100)

    lines = open('numbers.txt', "r").readlines()
    latest_line = lines[-1]
    if(latest_line.find(' ')<0):
        lines = lines[:-1]
    lines.append(str(num_1)+' '+str(num_2) + '\n')
    open('numbers.txt', "w").writelines(lines)

def calculate():
    sum_1 = 0
    sum_2 = 0
    f = open('numbers.txt', "r")
    while True:
        line = f.readline()
        if not line:
            break
        nums = line.split()
        if(len(nums)>1):
            sum_1 += int(nums[0])
            sum_2 += int(nums[1])
    f.close()
    open('numbers.txt', "a").write(str(sum_2 - sum_1)+'\n')


with DAG(dag_id='first_dag', start_date=datetime(2022, 12, 20, 13, 1), end_date=datetime(2022, 12, 20, 13, 6), schedule="* * * * *") as dag:
    bash_task = BashOperator(task_id='hello', bash_command='echo hello')
    python_operator = PythonOperator(task_id='world', python_callable=hello)
    python_operator_num = PythonOperator(task_id='add_num', python_callable=generate_num)
    python_operator_calc: PythonOperator = PythonOperator(task_id='calc', python_callable=calculate)

    bash_task >> python_operator >> python_operator_num >> python_operator_calc