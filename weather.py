import requests
import json


APPID = '4e9a0812c9fea74873e7a091fba8d505'
URL = f'https://api.openweathermap.org/data/2.5/weather?q=Cheboksary,ru&APPID={APPID}&units=metric'


def download_weather():
    response = requests.get(url=URL).json()
    name = response['name']                     # название города
    temperature = response['main']['temp']      # температура
    pressure = response['main']['pressure']     # АТ давление
    humidity = response['main']['humidity']     # влажность воздуха
    json_str = f'city = {name}; temperature = {temperature} °C; ' \
               f'pressure = {pressure} мм.рт.ст.; humidity = {humidity} %'

    with open('weather_json.txt', 'w') as f:
        json.dump(json_str, f)

    with open('weather_json.txt') as f:
        print(f.read())

    return json_str


print(download_weather())
