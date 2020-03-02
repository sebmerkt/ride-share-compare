#!/bin/python3

import psycopg2
from psycopg2 import Error
import os

try:
  connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                password = os.getenv("DB_PW"),
                                host = os.getenv("DB_SERVER"),
                                port = os.getenv("DB_PORT"),
                                database = os.getenv("DB_NAME"))

  cursor = connection.cursor()
  
  create_table_query = '''CREATE TABLE ride_share_data
        ( id SERIAL UNIQUE PRIMARY KEY,
          uuid TEXT,
          vendor_name TEXT,
          Trip_Pickup_DateTime timestamp,
          Trip_Dropoff_DateTime timestamp,
          Passenger_Count int8,
          Trip_Distance float8,
          Start_Lon double precision,
          Start_Lat double precision,
          PULocationID int8,
          Rate_Code float8,
          store_and_forward float8,
          End_Lon double precision,
          End_Lat double precision,
          DOLocationID int8,
          Fare_Amt float8,
          Payment_Type TEXT,
          Tolls_Amt float8, 
          Tip_Amt float8,
          surcharge float8,
          mta_tax float8,
          extra float8,
          Total_Amt float8,
          trip_duration int8,
          start_station_id int8,
          start_station_name text,
          end_station_id int8,
          end_station_name text,
          bike_id int8,
          user_type text,
          birth_year int8,
          gender int8,
          Process_time timestamp,
          geom_start geometry(Point, 4326),
          geom_end geometry(Point, 4326)); '''

  cursor.execute(create_table_query)
  connection.commit()

except (Exception, psycopg2.DatabaseError) as error :
  print ("Error while creating PostgreSQL table ride_share_data.", error)
finally:
  #closing database connection.
    if(connection):
      cursor.close()
      connection.close()
      
      
      