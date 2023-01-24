import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date
from datetime import datetime, timedelta
import io
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': '_________',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
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
def dag_alerts_malkova():
    
    @task
    def feed_check_anomaly(df, metric, a = 4, n = 5): 
        chat_id = chat or 236759627
        bot = telegram.Bot(token='5793676769:AAHOEwUkBz2RBunrjuKqA1SFBYVMhQvJEuM')
        
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['up'] = df['q75'] + a * df['iqr']
        df['low'] = df['q25'] - a * df['iqr']

        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df

    @task
    def feed_alerts(chat = None):
        query_feed = """SELECT 
                        toStartOfFifteenMinutes(time) as ts,
                        toDate(time) as date,
                        formatDateTime(ts, '%R') as hm,
                        uniqExact(user_id) as users_feed,
                        countIf(user_id, action='view') as views,
                        countIf(user_id, action='like') as likes,
                        likes/views as CTR
                        FROM simulator_20220920.feed_actions
                        WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm 
                        ORDER BY ts"""
        
        df_feed = ph.read_clickhouse(query=query_feed, connection=connection)
        return df_feed
    
    metrics_list_feed = ['users_feed', 'views', 'likes', 'CTR']
    for metric in metrics_list_feed:
        print(metric)
        df = df_feed[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = feed_check_anomaly(df, metric)

        if is_alert == 1:
            msg = """Метрика {metric}:\nТекущее значение {current_val:.2f}\nОтклонение от предыдущего значения {last_val_diff:.2f}\nhttp://superset.lab.karpov.courses/r/2202\n@eyereen""".format(metric=metric,
                     current_val=df[metric].iloc[-1], last_val_diff=1 - (df[metric].iloc[-1]/df[metric].iloc[-2]))    

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x = df['ts'], y = df[metric], label = 'metric')
            ax = sns.lineplot(x = df['ts'], y = df['up'], label = 'up')
            ax = sns.lineplot(x = df['ts'], y = df['low'], label = 'low')

            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel='time')
            ax.set(ylabel=metric)

            ax.set_title(metric)
            ax.set(ylim = (0, None)) #нижняя граница у = 0

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'metrics_plot.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)   

            #bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendMessage(chat_id=chat_id, text=msg)

    return

    feed_alerts(chat = None)

    
    @task
    def mess_check_anomaly(df, metric, a = 1, n = 2):
        chat_id = chat or 236759627
        bot = telegram.Bot(token='5793676769:AAHOEwUkBz2RBunrjuKqA1SFBYVMhQvJEuM')
        
        df['rolling'] = df[metric].shift(1).rolling(n).mean()
        df['std'] = df[metric].shift(1).std()
        df['up'] = df['rolling'] + a * df['std']
        df['low'] = df['rolling'] - a * df['std']

        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

        if df[metric].iloc[-1] < df['up'].iloc[-1] or df[metric].iloc[-1] > df['low'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df
    
    
    @task
    def messages_alerts(chat = None) :#правило сигм
        query_mess = """SELECT 
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(time) as date,
                    formatDateTime(ts, '%R') as hm,
                    uniqExact(user_id) as users_mess,
                    count(reciever_id) as sent_messages
                    FROM simulator_20220920.message_actions
                    WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm 
                    ORDER BY ts"""
        
        df = ph.read_clickhouse(query=query_mess, connection=connection)
        return df
    
        metrics_list_messages = ['users_mess', 'sent_messages']
        for metric in metrics_list_messages:
            print(metric)
            df = data_mess[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = mess_check_anomaly(df, metric)

            if is_alert == 1:
                msg = """Метрика {metric}:\nТекущее значение {current_val:.2f}\nОтклонение от предыдущего значения {last_val_diff:.2f}""".format(metric=metric,
                         current_val=df[metric].iloc[-1], last_val_diff=1 - (df[metric].iloc[-1]/df[metric].iloc[-2]))   

                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()

                ax = sns.lineplot(x = df['ts'], y = df[metric], label = 'metric')
                ax = sns.lineplot(x = df['ts'], y = df['up'], label = 'up')
                ax = sns.lineplot(x = df['ts'], y = df['low'], label = 'low')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time')
                ax.set(ylabel=metric)

                ax.set_title(metric)
                ax.set(ylim = (0, None)) #нижняя граница у = 0

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'metrics_plot.png'
                plt.close()
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)       



            bot.sendMessage(chat_id=chat_id, text=msg)
        
    messages_alerts(chat = None)

df_feed = feed_alerts()
mess_check_anomaly(df, metric)
df = messages_alerts()
feed_check_anomaly(df, metric) 
    
dag_alerts_malkova = dag_alerts_malkova()
