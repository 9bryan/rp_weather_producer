import requests
import json
import time
import configparser
from uuid import uuid4
from kafka import KafkaProducer

#Load big list of all cities into a dictionary
with open('city.list.json') as json_file:
    all_cities = json.load(json_file)

#load configurations from config.ini
config = configparser.ConfigParser()
config.read("config.ini")
api_key = config.get("default", "api_key")
weather_topic = config.get("default", "weather_topic")
forecast_topic = config.get("default", "forecast_topic")
rp_brokers = json.loads(config.get("default", "rp_brokers"))
schema_registry_url = config.get("default", "schema_registry_url")
weather_schema = config.get("default", "weather_schema")
forecast_schema = config.get("default", "forecast_schema")

def get_weather(city):
    lat = city['coord']['lat']
    lon = city['coord']['lon']
    weather_request = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}")
    return json.loads(weather_request.content.decode('utf-8'))

def get_forecast(city):
    lat = city['coord']['lat']
    lon = city['coord']['lon']
    weather_request = requests.get(f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}")
    return json.loads(weather_request.content.decode('utf-8'))

def get_cities():
    config = configparser.ConfigParser()
    config.read("config.ini")
    city_ids = json.loads(config.get("default", "cities"))
    cities = []
    for city_id in city_ids:
        city = list(filter(lambda city: city['id'] == city_id, all_cities))[0]
        cities.append(city)
    return cities

def produce_weathers():
    cities = get_cities()
    weather_producer = KafkaProducer(bootstrap_servers=rp_brokers)
    for city in cities:
        weather = get_weather(city)
        weather_producer.send(weather_topic, bytes(str(weather), 'utf-8'))
        time.sleep(1)

def produce_forecasts():
    cities = get_cities()
    forecast_producer = KafkaProducer(bootstrap_servers=rp_brokers)
    for city in cities:
        forecast = get_forecast(city)
        forecast_producer.send(forecast_topic, bytes(str(forecast), 'utf-8'))
        forecast_producer.flush()
        time.sleep(1)

# TESTS

#produce_weathers()
#produce_forecasts()
#print(*get_cities())

#medford = '''{    
#    "id": 5740099,
#    "name": "Medford",                                              
#    "state": "OR",
#    "country": "US",       
#    "coord": {  
#        "lon": -122.875587,
#        "lat": 42.326519
#    }                    
#}'''
#print(json.dumps(get_weather(json.loads(medford))))
#print(json.dumps(get_forecast(json.loads(medford))))
