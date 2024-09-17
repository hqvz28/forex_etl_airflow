from datetime import datetime
import json
import csv

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SYMBOLS = Variable.get("SYMBOLS")
API_KEY = Variable.get("API_KEY")
BOT_TOKEN = Variable.get("BOT_TOKEN")
CHAT_ID = Variable.get("CHAT_ID")
BASE_URL = "https://api.apilayer.com/exchangerates_data"
BASE_CURRENCY = "USD"


def fetch_exchange_rate_data(**context):
    try:
        url = f"{BASE_URL}/{context['ds']}?symbols={SYMBOLS}&base={BASE_CURRENCY}"
        headers = {"apikey": API_KEY}
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        data = response.json()
        file_path = f"/opt/airflow/data/exchange_rates_{context['ds']}.json"
        with open(file_path, 'w') as outfile:
            json.dump(data, outfile)
    except requests.RequestException as e:
        print(f"Request failed: {e}")
    except IOError as e:
        print(f"File operation failed: {e}")


def insert_upsert_data(**context):
    try:
        hook = PostgresHook(postgres_conn_id='POSTGRES_CONN_ID')
        conn = hook.get_conn()
        cursor = conn.cursor()

        file_path = f"/opt/airflow/data/exchange_rates_{context['ds']}.json"
        with open(file_path, 'r') as data_file:
            data = json.load(data_file)

        df = pd.DataFrame(data['rates'].items(), columns=['currency', 'rate'])
        df['date'] = data['date']
        df['base'] = data['base']

        upsert_query = """
        INSERT INTO exchange_rates (base, date, currency, rate)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date, base, currency) DO UPDATE
        SET rate = EXCLUDED.rate;
        """
        cursor.executemany(upsert_query, df[['base', 'date', 'currency', 'rate']].values.tolist())

        conn.commit()
    except (IOError, json.JSONDecodeError) as e:
        print(f"File operation or JSON decoding failed: {e}")
    except Exception as e:
        print(f"Database operation failed: {e}")
    finally:
        cursor.close()
        conn.close()


def analyze_exchange_rates(**context):
    try:
        hook = PostgresHook(postgres_conn_id='POSTGRES_CONN_ID')
        engine = hook.get_sqlalchemy_engine()

        query = "SELECT base, date, currency, rate FROM exchange_rates"
        df = pd.read_sql(query, con=engine)
        df['date'] = pd.to_datetime(df['date'])
        result_df = pd.DataFrame(columns=['currency', 'current_date', 'max_deviation_date', 'max_deviation_value'])

        current_date = context['ds'] 
        current_date = pd.to_datetime(current_date)

        for currency, group in df.groupby('currency'):
            group = group[group['date'] <= current_date]

            current_rate = group.loc[group['date'] == current_date, 'rate']
            if current_rate.empty:
                continue  

            current_rate = current_rate.iloc[0]  
            group['rate_diff'] = abs(group['rate'] - current_rate)

            max_deviation_row = group.loc[group['rate_diff'].idxmax()]
            max_deviation_date = max_deviation_row['date']
            max_deviation_value = max_deviation_row['rate_diff']

            result_df = result_df.append({
                'currency': currency,
                'current_date': current_date.strftime('%Y-%m-%d'),
                'max_deviation_date': max_deviation_date.strftime('%Y-%m-%d'),
                'max_deviation_value': max_deviation_value
            }, ignore_index=True)

        result_df.to_csv('/opt/airflow/data/max_deviation.csv', index=False)
        print("Data has been successfully saved to max_deviation.csv.")

    except Exception as e:
        print(f"Data analysis failed: {e}")


def send_csv_to_telegram(bot_token, chat_id, file_path, message):
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
        with open(file_path, 'rb') as file:
            payload = {
                'chat_id': chat_id,
                'caption': message
            }
            files = {
                'document': file
            }
            response = requests.post(url, data=payload, files=files)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            if response.status_code == 200:
                print("File CSV đã được gửi thành công!")
            else:
                print(f"Lỗi xảy ra: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        print(f"Telegram API request failed: {e}")
    except IOError as e:
        print(f"File operation failed: {e}")
    except Exception as e:
        print(f"Error sending file: {e}")


with DAG(
    'forex_etl_pipeline',
    schedule_interval='0 1 * * *',
    catchup=True,
    max_active_run=1,
    start_date=datetime(2024, 9, 1)
) as dag:

    task1 = PythonOperator(
        task_id='fetch_exchange_rate_data',
        python_callable=fetch_exchange_rate_data
    )

    task2 = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="POSTGRES_CONN_ID",
        sql="""
        CREATE TABLE IF NOT EXISTS exchange_rates (
            base VARCHAR(3),
            date DATE,
            currency VARCHAR(3),
            rate FLOAT,
            PRIMARY KEY (date, base, currency)
        );
        """
    )

    task3 = PythonOperator(
        task_id='insert_upsert_data',
        python_callable=insert_upsert_data
    )

    task4 = PythonOperator(
        task_id='analyze_exchange_rates',
        python_callable=analyze_exchange_rates
    )

    task5 = PythonOperator(
        task_id='send_csv_to_telegram',
        python_callable=send_csv_to_telegram,
        op_kwargs={
            'bot_token': BOT_TOKEN,
            'chat_id': CHAT_ID,
            'file_path': "/opt/airflow/data/max_deviation.csv",
            'message': "Gửi ông chủ đáng kính!"
        }
    )

    task1 >> task2 >> task3 >> task4 >> task5
