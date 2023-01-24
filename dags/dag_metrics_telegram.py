import telegram
from datetime import datetime, timedelta
import pandahouse as ph
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pylab

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '________',
    'user': 'student',
    'database': 'simulator_20220920'
}

# Дефолтные параметры
default_args = {
    'owner': 'i.malkova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 16),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False )
def dag_malkova_report():
    
    @task
    def extract_feed():
        query_feed = """
            SELECT 
                    user_id,
                    toDate(time) as date,
                    countIf(user_id, action='view') as views,
                    countIf(user_id, action='like') as likes,
                    likes/views as CTR,
                    source
            FROM simulator_20220920.feed_actions
            GROUP BY user_id,
                    date,
                    source
            """
        df_feed = ph.read_clickhouse(query=query_feed, connection=connection)
        return df_feed
    
    @task
    def extract_message():
        query_feed = """
            SELECT 
                    user_id,
                    toDate(time) as date,
                    countIf(user_id, action='view') as views,
                    countIf(user_id, action='like') as likes,
                    likes/views as CTR,
                    source
            FROM simulator_20220920.feed_actions
            GROUP BY user_id,
                    date,
                    source
            """
        df_feed = ph.read_clickhouse(query=query_feed, connection=connection)
        return df_feed

    @task
    def load(df_cube):
        chat_id = -769752736

        my_token = '5793676769:AAHOEwUkBz2RBunrjuKqA1SFBYVMhQvJEuM' 
        bot = telegram.Bot(token=my_token)

        msg =   f'Дата: {df_cube.event_day[6].date()}\n\n' \
                f'DAU: {df_cube.DAU[6]}\n' \
                f'Просмотры: {df_cube.views[6]}\n' \
                f'Лайки: {df_cube.likes[6]}\n' \
                f'CTR: {df_cube.CTR[6].round(3)}'
        bot.sendMessage(chat_id = chat_id, text = msg)
        
        short_date = df_cube.event_day.dt.strftime('%m-%d')
        
        plt.figure(figsize=(17, 10))

        pylab.subplot(221)
        plt.plot(short_date, df_cube.DAU, label='DAU')
        plt.legend()
        plt.title('Daily active users', fontweight="bold", size=15)

        pylab.subplot(223)
        plt.plot(short_date, df_cube.views, label='views')
        plt.legend()
        plt.title('Views', fontweight="bold", size=15)

        pylab.subplot(222)
        plt.plot(short_date, df_cube.likes, label='likes')
        plt.legend()
        plt.title('Likes', fontweight="bold", size=15)

        pylab.subplot(224)
        plt.plot(short_date, df_cube.CTR, label='CTR')
        plt.legend()
        plt.title('Click to rate', fontweight="bold", size=15)

        plt.subplots_adjust(left=0.125,
                    bottom=0.1, 
                    right=0.9, 
                    top=0.9, 
                    wspace=0.2, 
                    hspace=0.35)

        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)  
        
    df_cube = extract()
    load(df_cube)
        
dag_malkova_report=dag_malkova_report()
