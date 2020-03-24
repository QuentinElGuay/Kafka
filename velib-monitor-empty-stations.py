from collections import defaultdict
import json

from kafka import KafkaConsumer


empty_stations_per_city = defaultdict(int)
consumer = KafkaConsumer('empty-stations', bootstrap_servers='localhost:9092', group_id='velib-monito-emptyr-stations')

for message in consumer:
    station=json.loads(message.value.decode())
    station_address = station['address']
    contract = station['contract_name']
    available_bikes = station['available_bikes']

    if available_bikes == 0:
        empty_stations_per_city[contract] += 1
        print(f'Station {station_address} of {contract} ({empty_stations_per_city[contract]}) is now empty.')

    elif empty_stations_per_city[contract] > 0:
        empty_stations_per_city[contract] -= 1
