import requests
import psycopg2
import json
from psycopg2 import OperationalError


def download():
    import key_appid as key  # импорт файла с ключом
    key_appid = key.key_appid
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={key_appid}&units=metric'
    r = requests.get(url=url).json()

    with open('weather_city.json', 'w') as filename:
        json.dump(r, filename)
    print("Файл успешно скачан")

    return r


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
                        id_weather int4 GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        coord json,
                        weather json,
                        base text,
                        main json,
                        visibility text,
                        wind json,
                        clouds json,
                        dt int4,
                        sys json,
                        timezone int4,
                        id int4,
                        name text,
                        cod int4
                        ); '''
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица успешно создана")
    except OperationalError as e:
        print(f"Произошла ошибка {e}")


def insert_json_db(r):
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        print("Подключение к базе PostgreSQL для добавления json выполнено")
        cursor = connection.cursor()
        cursor.execute("INSERT INTO public.weather (coord,weather,base,main,visibility,wind,clouds,dt,sys,timezone,id,name,cod)"
                       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                       (json.dumps(r["coord"]), json.dumps(r["weather"]), r["base"], json.dumps(r["main"]), r["visibility"], json.dumps(r["wind"]),
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


create_connection_db()
resp = download()
insert_json_db(resp)
