import pika
import sys
import os
import requests
import psycopg2
import json
from psycopg2 import OperationalError
from datetime import datetime
import pytz


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='connection_db', durable=True)  # нужно убедиться, что очередь переживет перезапуск RabbitMQ, для этого нам нужно объявить его устойчивым

    def do_work(ch, method, properties, body):
        create_connection_db()
        date, time, resp = download()
        insert_json_db(date, time, resp)

    channel.basic_consume(queue='connection_db', on_message_callback=do_work, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def download():
    import key_appid as key  # импорт файла с ключом
    key_appid = key.key_appid
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid}&units=metric'
    r = requests.get(url=url).json()
    msc = pytz.timezone('europe/moscow')
    date_downloads = datetime.now(msc).strftime("%Y-%m-%d")
    time_downloads = datetime.now(msc).strftime("%H:%M:%S")

    with open('weather_city.json', 'w') as filename:
        json.dump(r, filename)
    print("Файл успешно скачан")

    return date_downloads, time_downloads, r


def create_connection_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="db_app",  # название контейнера в docker-compose
                                      port="5432",
                                      database="postgres")
        print("Подключение к базе PostgreSQL выполнено")
        cursor = connection.cursor()
        create_table_query = '''CREATE TABLE IF NOT EXISTS public.weather (
                        id_weather int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        date_downloads date,
                        time_downloads time,
                        coord json,
                        weather json,
                        base text,
                        main json,
                        visibility text,
                        wind json,
                        clouds json,
                        dt int,
                        sys json,
                        timezone int,
                        id int,
                        name text,
                        cod int
                        ); '''

        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица успешно создана")
    except OperationalError as e:
        print(f"Произошла ошибка {e}")


def insert_json_db(date_downloads, time_downloads, r):
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="db_app",  # название контейнера в docker-compose
                                      port="5432",
                                      database="postgres")
        print("Подключение к базе PostgreSQL для добавления json выполнено")
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO public.weather (date_downloads,time_downloads,coord,weather,base,main,visibility,wind,clouds,dt,sys,timezone,id,name,cod)"
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (date_downloads, time_downloads,
             json.dumps(r["coord"]), json.dumps(r["weather"]), r["base"], json.dumps(r["main"]), r["visibility"],
             json.dumps(r["wind"]),
             json.dumps(r["clouds"]), r["dt"], json.dumps(r["sys"]), r["timezone"], r["id"], r["name"], r["cod"]))
        connection.commit()
        count = cursor.rowcount
        print(count, "Запись успешно вставлена в таблицу")
    except (Exception, psycopg2.Error) as error:
        print("Не удалось вставить данные в таблицу", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os.exit(0)
