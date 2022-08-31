from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
import re


default_args={
        'start_date': datetime(2022, 8, 27),
        'owner':'Airflow'
    }

dag = DAG(
    "first_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)


def read_csv():
    df = pd.read_csv('https://drive.google.com/file/d/1ryGW5QJIpCClfottA4ljaJQYQgz-WSwF/view?usp=sharing, encoding='UTF-8')
    df.to_csv('~/Documents/GitHub/Task_5/data/output_0.csv', index=False)

download_df = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag
)


def drop_dubl():
    df = pd.read_csv('~/Documents/GitHub/Task_5/data/output_0.csv')
    df.drop_duplicates(inplace=True)
    df.to_csv('~/Documents/GitHub/Task_5/data/output_1.csv', index=False)

drop_dublicates = PythonOperator(
    task_id='drop_dubl',
    python_callable=drop_dubl,
    dag=dag
)


def drop_unset():
    df = pd.read_csv('~/Documents/GitHub/Task_5/data/output_1.csv')
    df.dropna(thresh=2)
    df.to_csv('~/Documents/GitHub/Task_5/data/output_2.csv', index=False)

drop_unset = PythonOperator(
    task_id='drop_unset',
    python_callable=drop_unset,
    dag=dag
)


def replace_null():
    df=pd.read_csv('~/Documents/GitHub/Task_5/data/output_2.csv')
    df=df.fillna('-')
    df.to_csv('~/Documents/GitHub/Task_5/data/output_3.csv', index=False)

replace_null = PythonOperator(
    task_id='replace_null',
    python_callable=replace_null,
    dag=dag
)


def rename_columns():
    df = pd.read_csv('~/Documents/GitHub/Task_5/data/output_3.csv')
    df = df.rename(columns={'reviewId': 'id', 'userName': 'user_name', 'userImage': 'user_image', 'content': 'comment',
                            'thumbsUpCount': 'thumbs_up_count', 'reviewCreatedVersion': 'version', 'at': 'date_comment',
                            'replyContent': 'reply_comment',
                            'repliedAt': 'reply_date'})
    df.to_csv('~/Documents/GitHub/Task_5/data/output_4.csv', index=False)

rename_columns = PythonOperator(
    task_id='rename_columns',
    python_callable=rename_columns,
    dag=dag
)


def object_to_date():
    df = pd.read_csv('~/Documents/GitHub/Task_5/data/output_4.csv')
    df['date_comment'] = pd.to_datetime(df['date_comment'], errors='ignore', format='%Y-%m-%d %H:%M:%S')
#   df['date_comment'] = df['date_comment'].values.astype(np.int64) // 10 ** 9
    df['reply_date'] = pd.to_datetime(df['reply_date'], errors='ignore', format='%Y-%m-%d %H:%M:%S')
    df.to_csv('~/Documents/GitHub/Task_5/data/output_5.csv', index=False)

object_to_date = PythonOperator(
    task_id='object_to_date',
    python_callable=object_to_date,
    dag=dag
)

def sort_date():
    df = pd.read_csv('~/Documents/GitHub/Task_5/data/output_5.csv')
    df = df.sort_values(by=['date_comment'], ascending=False)
    df.to_csv('~/Documents/GitHub/Task_5/data/output_6.csv', index=False)

sort_date = PythonOperator(
    task_id='sort_date',
    python_callable=sort_date,
    dag=dag
)


def str_to_lower():
    df = pd.read_csv('~/Documents/GitHub/Task_5/data/output_6.csv')
    df['user_name'] = df['user_name'].str.lower()
    df['comment'] = df['comment'].str.lower()
    df['reply_comment'] = df['reply_comment'].str.lower()
    df.to_csv('~/Documents/GitHub/Task_5/data/output_7.csv', index=False)

str_to_lower = PythonOperator(
    task_id='str_to_lower',
    python_callable=str_to_lower,
    dag=dag
)


def remove_emoji():
    df = pd.read_csv('~/Documents/GitHub/Task_5/data/output_7.csv')
    df['comment'] = df['comment'].astype(str).apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    df['reply_comment'] = df['reply_comment'].astype(str).apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    df['comment'] = df['comment'].str.replace('\n', '')
    df['comment'] = df['comment'].str.replace('\t', ' ')
    df['comment'] = df['comment'].str.replace(' {2,}', ' ', regex=True)
    df['comment'] = df['comment'].str.strip()
    df['reply_comment'] = df['reply_comment'].str.replace('\n', '')
    df['reply_comment'] = df['reply_comment'].str.replace('\t', ' ')
    df['reply_comment'] = df['reply_comment'].str.replace(' {2,}', ' ', regex=True)
    df['reply_comment'] = df['reply_comment'].str.strip()
    df.to_csv('~/Documents/GitHub/Task_5/data/output_8.csv', index=False)

remove_emoji = PythonOperator(
    task_id='remove_emoji',
    python_callable=remove_emoji,
    dag=dag
)

import_mongo = 'mongoimport --db=Task_5 --collection=Comments --type=csv --headerline --file=/home/ivannikalayeu/Documents/GitHub/Task_5/data/output_8.csv --drop'

import_m = BashOperator(
    task_id='import_mongo',
    bash_command=import_mongo,
    dag=dag
)

download_df >> drop_dublicates >> drop_unset >> replace_null >> rename_columns >> object_to_date >> sort_date >> str_to_lower >> remove_emoji >> import_m



client = MongoClient('mongodb://localhost:27017/')
db = client.Task_5
coll = db.Comments

# Top 5 famous commentaries
filter={}
sort=list({
    'thumbs_up_count': -1
}.items())
limit=5

result_0 = client['Task_5']['Comments'].find(
  filter=filter,
  sort=sort,
  limit=limit
)

# All records, where length of field “content” is less than 5 characters

filter={
    'comment': {
        '$regex': re.compile(r"^.{0,5}$")
    }
}

result_1 = client['Task_5']['Comments'].find(
  filter=filter
)

# Avarage rating by each day
result = client['Task_5']['Comments'].aggregate([
    {
        '$group': {
            '_id': {
                'days': {
                    '$dayOfYear': '$date_comment'
                }
            },
            'AvgRating': {
                '$avg': '$score'
            }
        }
    }
])

