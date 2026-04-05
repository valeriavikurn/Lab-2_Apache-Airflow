from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'student2',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'student_grades_pipeline',
    default_args=default_args,
    description='Пайплайн обработки студенческих оценок',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['lab2', 'grades'],
)

def collect_grades(**context):
    students = {
        'Алексей': random.randint(50, 100),
        'Мария': random.randint(50, 100),
        'Дмитрий': random.randint(50, 100),
        'Анна': random.randint(50, 100),
        'Иван': random.randint(50, 100),
    }
    print(f"Собранные оценки: {students}")
    context['ti'].xcom_push(key='grades', value=students)

def analyze_grades(**context):
    grades = context['ti'].xcom_pull(key='grades', task_ids='collect_grades')
    avg = sum(grades.values()) / len(grades)
    best = max(grades, key=grades.get)
    worst = min(grades, key=grades.get)
    result = {
        'average': round(avg, 2),
        'best_student': best,
        'best_score': grades[best],
        'worst_student': worst,
        'worst_score': grades[worst],
        'passed': {k: v for k, v in grades.items() if v >= 60},
    }
    print(f"Анализ: {result}")
    context['ti'].xcom_push(key='analysis', value=result)

def save_report(**context):
    analysis = context['ti'].xcom_pull(key='analysis', task_ids='analyze_grades')
    with open('/tmp/grades_report.txt', 'w') as f:
        f.write("=== ОТЧЁТ ПО ОЦЕНКАМ СТУДЕНТОВ ===\n")
        f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Средний балл: {analysis['average']}\n")
        f.write(f"Лучший студент: {analysis['best_student']} ({analysis['best_score']})\n")
        f.write(f"Худший результат: {analysis['worst_student']} ({analysis['worst_score']})\n")
        f.write(f"Сдали экзамен: {list(analysis['passed'].keys())}\n")
    print("Отчёт сохранён в /tmp/grades_report.txt")

task1 = PythonOperator(task_id='collect_grades', python_callable=collect_grades, dag=dag)
task2 = PythonOperator(task_id='analyze_grades', python_callable=analyze_grades, dag=dag)
task3 = PythonOperator(task_id='save_report', python_callable=save_report, dag=dag)
task4 = BashOperator(task_id='print_report', bash_command='cat /tmp/grades_report.txt', dag=dag)

task1 >> task2 >> task3 >> task4