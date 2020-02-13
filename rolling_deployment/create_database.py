#!/bin/python

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
        ( uuid uuid UNIQUE,
          vendor_name TEXT,
          Trip_Pickup_DateTime timestamp,
          Trip_Dropoff_DateTime timestamp,
          Trip_Distance float8,
          Start_Lon double precision,
          Start_Lat double precision,
          End_Lon double precision,
          End_Lat double precision,
          Total_Amt float8,
          Process_time timestamp,
          geom_start geometry(Point, 4326),
          geom_end geometry(Point, 4326),
          PRIMARY KEY (uuid)); '''

  cursor.execute(create_table_query)
  connection.commit()

except (Exception, psycopg2.DatabaseError) as error :
  print ("Error while creating PostgreSQL table ride_share_data.", error)
finally:
  #closing database connection.
    if(connection):
      cursor.close()
      connection.close()