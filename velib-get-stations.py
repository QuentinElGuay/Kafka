import json
import time
from  urllib import request


from kafka import KafkaProducer

API_KEY = 'aa27a3e80b5a62d5e41fb4820f8377f912d94726'

url = f'https://api.jcdecaux.com/vls/v1/stations?apiKey={API_KEY}'


producer = KafkaProducer(bootstrap_servers="localhost:9092")
empty_stations = {}

while True:
    response = request.urlopen(url)
    stations = json.loads(response.read().decode())
    prev_empty_stations = empty_stations
    empty_stations = set()

    for station in stations:
        # Send info for all stations
        producer.send('velib-stations', json.dumps(station).encode(), key=str(station["number"]).encode())

        # Detect empty stations 
        station_number = station['number']
        contract = station['contract_name']

        code = contract + str(station_number)
        if station['available_bikes'] == 0:
            if code not in prev_empty_stations:
                producer.send('empty-stations', json.dumps(station).encode(), key=str(station["number"]).encode())
            empty_stations.add(code)
        elif code not in prev_empty_stations:
            producer.send('empty-stations', json.dumps(station).encode(), key=station["contract_name"].encode())
    
    print(f'{time.time()} Produced {len(stations)} station records.')
    print(f'and {len(empty_stations)} "empty" station records.') 
    time.sleep(1)