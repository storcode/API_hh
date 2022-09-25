class WeatherApi:
    apiUrl = 'https://api-url'
    apiToken = 'asdfas9d8f6ya0s9dfpyhapsodfljl'
    urlLib = None

    def __init__(self, urllib):
        self.urlLib = urllib

    def load_city_weather_data(self, city_name):
        url = self.apiUrl + '?' + city_name + '?' + self.apiToken
        data = self.urlLib.get(url)
        return data

    def get_country_month_stats(self, country_name):
        url = self.apiUrl + 'by_contry?' + country_name
