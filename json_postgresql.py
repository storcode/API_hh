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


def create_connection_db():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        print("Подключение к базе PostgreSQL выполнено")
        cursor = connection.cursor()
        create_table_query = '''CREATE TABLE IF NOT EXISTS public.city (
                        id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        name VARCHAR(255)
                        );
                        CREATE TABLE IF NOT EXISTS public.weather_data (
                        id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        city_id INTEGER,
                        temperature FLOAT,
                        pressure INTEGER,
                        humidity INTEGER,
                        FOREIGN KEY(city_id) REFERENCES city(id)
                            ON DELETE RESTRICT ON UPDATE RESTRICT
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
        cursor.execute("INSERT INTO public.weather_data VALUES ('{}')".format(json.dumps(r)))
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
insert_json_db(download())
