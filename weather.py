import requests
import json


APPID = '4e9a0812c9fea74873e7a091fba8d505'
city = 'Cheboksary'
units = 'metric' # температура по Цельсию
URL = f'https://api.openweathermap.org/data/2.5/weather?q={city},ru&APPID={APPID}&units={units}'


def download_weather():
    response = requests.get(url=URL).json()
    json_str = response['name'] + ': ' + str(response['main']['temp'])

    with open('weather_json.txt', 'w') as f:
        json.dump(json_str, f)

    with open('weather_json.txt') as f:
        print(f.read())

    return json_str


print(download_weather())
