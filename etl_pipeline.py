# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# параметры для соединения с CH
connection = {
    'host': '*****',
    'password': '*****',
    'user': '*****',
    'database': '*****'
}

# функция для CH
def ch_get_df(query, con):
    result = ph.read_clickhouse(query, connection=con)
    return result

# функция для группировки наших данных
def groupby(data, column):
    # 1 группируем по нужному столбцу и суммируем метрики
    result = (
        data.groupby(['date', column])
        .agg({
            'views': 'sum',
            'likes': 'sum',
            'messages_received': 'sum',
            'messages_sent': 'sum', 
            'users_received': 'sum',
            'users_sent': 'sum'})
        .reset_index()
        .rename(columns={column: 'dimension_value', 'date': 'event_date'})
    )
    # добавляем столбец с названием среза
    result['dimension'] = f'{column}'
    # упорядочиваем наши столбцы
    result = result[['event_date', 
                    'dimension', 
                    'dimension_value', 
                    'views', 
                    'likes', 
                    'messages_received', 
                    'messages_sent', 
                    'users_received', 
                    'users_sent']]
    # возвращаем результат
    return result

# функция для создания и добавления данных в таблицу
def load_data(df):
    
    # устанавливаем соединение 
    con = {
        'host': '*****',
        'database':'*****',
        'user':'*****',
        'password':'*****'
    }
    # пишем запрос на создание таблицы
    query = '''
    
    CREATE TABLE IF NOT EXISTS {db}.group_feed_action_message_iskl
    (
    event_date String,
    dimension String,
    dimension_value String,
    views Int32,
    likes Int32,
    messages_received Int32,
    messages_sent Int32,
    users_received Int32,
    users_sent Int32
    )
    ENGINE = MergeTree()
    ORDER BY event_date

    '''
    ph.execute(query=query, connection=con)
    ph.to_clickhouse(df,'group_feed_action_message_iskl', connection=con, index=False)

    


# зададим дефолтные параметры
default_args = {
    'owner': 'i-skljannyj',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 20)
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_group_feed_action_message():
    
     # получим нужные данные из ленты новостей
    @task
    def extract_feed_action():
        query_feed_action = '''
            SELECT toString(toDate(time)) AS date,
                   user_id, 
                   os, 
                   gender,
                   age,
                   countIf(action='view') AS views,
                   countIf(action='like') AS likes
            FROM simulator_20230220.feed_actions 
            GROUP BY date, user_id, os, gender,  age
            ORDER BY date, user_id, os, gender,  age
            '''
        feed_action = ch_get_df(query_feed_action, connection)
        return feed_action
    
    # получим нужные данные из мессенджера
    @task
    def extract_message():
        query_message = '''
            WITH
            send AS 
            (SELECT toString(toDate(time)) AS date, 
                   user_id,
                   os,
                   gender, 
                   age,
                   COUNT(reciever_id) AS messages_sent,
                   COUNT(DISTINCT reciever_id) AS users_sent
            FROM simulator_20230220.message_actions 
            GROUP BY date, user_id, os, gender, age
            ORDER BY date, user_id, os, gender, age),

            reciever AS 
            (SELECT toString(toDate(time)) AS date, 
                   reciever_id,
                   os,
                   gender, 
                   age,
                   COUNT(user_id) AS messages_received,
                   COUNT(DISTINCT user_id) AS users_received
            FROM simulator_20230220.message_actions 
            GROUP BY date, reciever_id, os, gender, age
            ORDER BY date, reciever_id, os, gender, age)

            SELECT s.date AS date,
                   s.user_id AS user_id,
                   s.os AS os,
                   s.gender AS gender,
                   s.age AS age,
                   r.messages_received AS messages_received,
                   s.messages_sent AS messages_sent,
                   r.users_received AS users_received,
                   s.users_sent AS users_sent
            FROM send AS s 
              JOIN reciever AS r 
                    ON s.date = r.date AND
                    s.user_id  = r.reciever_id AND
                    s.os = r.os AND
                    s.gender = r.gender AND
                    s.age = r.age
            '''
        message = ch_get_df(query_message, connection)
        return message
    
    #соединим наши таблицы
    @task
    def transform_tabels(feed_action, message):
        group_df = (
            feed_action
            .merge(message, on = ['date', 'user_id', 'os', 'gender', 'age'], how='outer')
            .fillna(0)
            .reset_index()
        )
        return group_df
    
    @task
    # сгруппируем наши данные по os
    def df_os(group_df):
        df_os = groupby(group_df, 'os')
        return df_os
    
    
    @task
    # сгруппируем наши данные по gender 
    def df_gender(group_df):
        df_gender = groupby(group_df, 'gender')
        return df_gender
    
    @task
    # сгруппируем наши данные по age
    def df_age(group_df):
        df_age = groupby(group_df, 'age')
        return df_age
    
    @task
    # объеденим таблицы 
    def concat_tabels(df_os, df_gender, df_age):
        concat_tabels = pd.concat([df_os, df_gender, df_age], axis=0)
        concat_tabels.loc[:, 'views':] = concat_tabels.loc[:, 'views':].astype('int32')
        
        return concat_tabels
    
    # загрузим наши данные в БД test
    @task
    def upload(concat_tabels):
        return load_data(concat_tabels)
   
    
    
    
    
    feed_action = extract_feed_action()
    message = extract_message()
    group_df = transform_tabels(feed_action, message)
    df_os =  df_os(group_df)
    df_gender = df_gender(group_df)
    df_age = df_age(group_df)
    concat_tabels = concat_tabels(df_os, df_gender, df_age)
    upload = upload(concat_tabels)

dag_group_feed_action_message = dag_group_feed_action_message()
