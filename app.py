import csv
from sqlalchemy import create_engine, Table, Column, String, Float, Integer, MetaData

engine = create_engine('sqlite:///weather_data.db', echo=True)
conn = engine.connect()

metadata = MetaData()
stations_table = Table('stations', metadata,
                       Column('station', String, primary_key=True),
                       Column('latitude', Float),
                       Column('longitude', Float),
                       Column('elevation', Float),
                       Column('name', String),
                       Column('country', String),
                       Column('state', String)
                       )
metadata.create_all(engine)

stations = []
with open('clean_stations.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        stations.append(
            {'station': row['station'], 'latitude': float(row['latitude']), 'longitude': float(row['longitude']),
             'elevation': float(row['elevation']), 'name': row['name'], 'country': row['country'],
             'state': row['state']})

conn.execute(stations_table.insert(), stations)

weather_table = Table('weather', metadata,
                      Column('station', String),
                      Column('date', String),
                      Column('precipitation', Float),
                      Column('temperature', Integer)
                      )
metadata.create_all(engine)

weather = []
with open('clean_measure.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        weather.append({'station': row['station'], 'date': row['date'], 'precipitation': float(row['precip']),
                        'temperature': int(row['tobs'])})

conn.execute(weather_table.insert(), weather)

result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
for row in result:
    print(row)

result = conn.execute("SELECT * FROM weather LIMIT 5").fetchall()
for row in result:
    print(row)
