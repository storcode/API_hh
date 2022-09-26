import requests
import psycopg2
import json
from psycopg2 import OperationalError
from datetime import datetime


def download():
    import key_appid as key  # импорт файла с ключом
    key_appid = key.key_appid
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid}&units=metric'
    r = requests.get(url=url).json()
    date_downloads = datetime.today().strftime("%Y-%m-%d")
    time_downloads = datetime.today().strftime("%H:%M:%S")
    print(date_downloads)
    print(time_downloads)

    with open('weather_city.json', 'w') as filename:
        json.dump(r, filename)
    print("Файл успешно скачан")

    return date_downloads, time_downloads, r


def create_connection_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="127.0.0.1",
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
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        print("Подключение к базе PostgreSQL для добавления json выполнено")
        cursor = connection.cursor()
        cursor.execute("INSERT INTO public.weather (date_downloads,time_downloads,coord,weather,base,main,visibility,wind,clouds,dt,sys,timezone,id,name,cod)"
                       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                       (date_downloads["date_downloads"], time_downloads["time_downloads"],
                        json.dumps(r["coord"]), json.dumps(r["weather"]), r["base"], json.dumps(r["main"]), r["visibility"], json.dumps(r["wind"]),
                        json.dumps(r["clouds"]), r["dt"], json.dumps(r["sys"]), r["timezone"], r["id"], r["name"], r["cod"]))
        connection.commit()
        count = cursor.rowcount
        print(count, "Запись успешно вставлена в таблицу")
        print("Соединение с PostgreSQL закрыто")
    except (Exception, psycopg2.Error) as error:
        print("Не удалось вставить данные в таблицу", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


create_connection_db()
date, time, resp = download()
insert_json_db(date, time, resp)
