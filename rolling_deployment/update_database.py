#!/usr/bin/python

# Example: Add two new columns column1, column2 with data types type1, type2
# ./update_database.py column1 type1 column2 type2

import sys
import os
import psycopg2
from psycopg2 import Error

for i in range(1,len(sys.argv),2):
  try:
    connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                  password = os.getenv("DB_PW"),
                                  host = os.getenv("DB_SERVER"),
                                  port = os.getenv("DB_PORT"),
                                  database = os.getenv("DB_NAME"))

    cursor = connection.cursor()
    
    create_table_query = '''ALTER TABLE ride_share_data
                            ADD COLUMN %s %s; '''%(sys.argv[i],sys.argv[i+1])

    cursor.execute(create_table_query)
    connection.commit()

  except (Exception, psycopg2.DatabaseError) as error :
    print ("Error while adding new fields to ride_share_data.", error)
  finally:
    #closing database connection.
    if(connection):
      cursor.close()
      connection.close()