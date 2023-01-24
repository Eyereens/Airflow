from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='_______'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

connection_simulator_20220920 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20220920',
    'user':'student',
    'password':'_______'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw',
    'password':'_______'
}

# Дефолтные параметры
default_args = {
    'owner': 'i.malkova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 12),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

query_feed_actions = """

SELECT 
    user_id as users,
    toDate(time) as event_date,
    countIf(user_id, action='like') as likes,
    countIf(user_id, action='view') as views,
    if(gender=1, 'male', 'female') as gender,
    multiIf(age < 18, '0 - 18', age >= 18 and age < 35, '18-35', '35+') as age,
    os
FROM {db}.feed_actions 
WHERE toDate(time) = today()-1 
GROUP BY 
    users,
    event_date,
    gender,
    os,
    age
"""

query_message_actions = """
SELECT users, 
       event_date, 
       users_sent, 
       messages_sent, 
       users_received, 
       messages_received,
       gender,
       os,
       age
FROM 
    (SELECT user_id as users,
            toDate(time) as event_date,
            COUNT(DISTINCT reciever_id) as users_sent,
            COUNT(reciever_id) as messages_sent,
            if(gender=1, 'male', 'female') as gender,
            multiIf(age < 18, '0 - 18', age >= 18 and age < 35, '18-35', '35+') as age,
            os
    FROM {db}.message_actions
    WHERE toDate(time) = today()-1 
    GROUP BY users,
             event_date,
             gender,
             os,
             age) as t1

    LEFT JOIN       

    (SELECT reciever_id as users,
            toDate(time) as event_date,
            COUNT(DISTINCT user_id) as users_received,
            COUNT(user_id) as messages_received
    FROM {db}.message_actions
    WHERE toDate(time) = today()-1 
    GROUP BY users,
             event_date) as t2

    ON ((t1.users=t2.users) AND (t1.event_date=t2.event_date))
format TSVWithNames
"""

query_new_table = """
CREATE TABLE IF NOT EXISTS test.i_malkova_etl
(
    event_date Date,
    dimension String,
    dimension_value String,
    views uint64,
    likes uint64,
    messages_received uint64,
    messages_sent uint64,
    users_received uint64,
    users_sent uint64,

) ENGINE = MergeTree()
"""


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False )
def dag_i_malkova():
    
    @task
    def extract():
        query_feed_actions = """
                        SELECT 
                            user_id as users,
                            toDate(time) as event_date,
                            countIf(user_id, action='like') as likes,
                            countIf(user_id, action='view') as views,
                            if(gender=1, 'male', 'female') as gender,
                            multiIf(age < 18, '0 - 18', age >= 18 and age < 35, '18-35', '35+') as age,
                            os
                        FROM {db}.feed_actions 
                        WHERE toDate(time) = today()-1 
                        GROUP BY 
                            users,
                            event_date,
                            gender,
                            os,
                            age
                        format TSVWithNames
                        """
        df_cube_1 = ch_get_df(query=query_feed_actions)
        return df_cube_1
    
    @task
    def extract():
        query_message_actions = """
                        SELECT users, 
                               event_date, 
                               users_sent, 
                               messages_sent, 
                               users_received, 
                               messages_received,
                               gender,
                               os,
                               age
                        FROM 
                            (SELECT user_id as users,
                                    toDate(time) as event_date,
                                    COUNT(DISTINCT reciever_id) as users_sent,
                                    COUNT(reciever_id) as messages_sent,
                                    if(gender=1, 'male', 'female') as gender,
                                    multiIf(age < 18, '0 - 18', age >= 18 and age < 35, '18-35', '35+') as age,
                                    os
                            FROM {db}.message_actions
                            WHERE toDate(time) = today()-1 
                            GROUP BY users,
                                     event_date,
                                     gender,
                                     os,
                                     age) as t1

                            LEFT JOIN       

                            (SELECT reciever_id as users,
                                    toDate(time) as event_date,
                                    COUNT(DISTINCT user_id) as users_received,
                                    COUNT(user_id) as messages_received
                            FROM {db}.message_actions
                            WHERE toDate(time) = today()-1 
                            GROUP BY users,
                                     event_date) as t2

                            ON ((t1.users=t2.users) AND (t1.event_date=t2.event_date))
                        format TSVWithNames
                        """
    
        df_cube_2 = ch_get_df(query=query_message_actions)
        return df_cube_2
    
    @task
    def merge(df_cube_1, df_cube_2):
        df_cubes = df_cube_1.merge(df_cube_2, how='outer', on=['users', 'event_date', 'os', 'age', 'gender']).fillna(0)
        return df_cubes
    
    @task
    def transfrom_age(df_cubes):
        df_cube_age = df_cubes \
            .rename(columns = {'age' : 'dimension_value'}) \
            .groupby(['event_date', 'dimension_value'], as_index=False) \
            .sum() \
            .drop('users', axis=1)

        df_cube_age['dimension']='age'
        return df_cube_age
    
    @task
    def transfrom_os(df_cubes):
        df_cube_os = df_cubes \
            .rename(columns = {'os' : 'dimension_value'}) \
            .groupby(['event_date', 'dimension_value'], as_index=False)\
            .sum() \
            .drop('users', axis=1)

        df_cube_os['dimension']='os'
        return df_cube_os
    
    @task
    def transfrom_gender(df_cubes):
        df_cube_gender = df_cubes \
            .rename(columns = {'gender' : 'dimension_value'}) \
            .groupby(['event_date', 'dimension_value'], as_index=False)\
            .sum() \
            .drop('users', axis=1)

        df_cube_gender['dimension']='gender'
        return df_cube_gender
    
    @task
    def union(df_cube_age, df_cube_os, df_cube_gender):
        df_cube_final = pd.concat([df_cube_age, df_cube_os, df_cube_gender], ignore_index=True)
        return df_cube_final
    
    @task
    def load(df_cube_final):
        context = get_current_context()
        ds = context['ds']
        print(f'Table with dimentions for {ds}')
        ph.to_clickhouse(df_cube_final, 'i_malkova_etl', index=False, connection=connection_test)
        
    df_cube_1 = extract()
    df_cube_2 = extract()
    df_cubes = merge(df_cube_1, df_cube_2)
    df_cube_age = transfrom_age(df_cubes)
    df_cube_os = transfrom_os(df_cubes)
    df_cube_gender = transfrom_gender(df_cubes)
    df_cube_final = union(df_cube_age, df_cube_os, df_cube_gender)
    load(df_cube_final)
    
dag_i_malkova= dag_i_malkova()
