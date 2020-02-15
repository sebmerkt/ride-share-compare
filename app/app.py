import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import psycopg2
from psycopg2 import Error
import plotly.express as px
import os
from datetime import datetime, timedelta
import geocoder
import time

import plotly.graph_objects as go



# get the mapbox token
token = open(os.getenv("MAPBOX_TOKEN")).read()

app = dash.Dash(
    __name__, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"]
)


app.layout = html.Div(
    [
        html.H1("Ride-Share-Compare"),
        dcc.Input(id='my-id', value='11 Wall Street, New York', type='text'),
        html.Div(id='my-div'),
        dcc.Interval(
            id='interval-component',
            interval=3*1000, # in milliseconds
            n_intervals=0
        ),
        dcc.Graph(id="graph", style={"width": "100%", "display": "inline-block"}),
    ]
)


@app.callback(
    Output(component_id='my-div', component_property='children'),
    [Input(component_id='my-id', component_property='value')]
)
def update_output_div(input_value):
  g = geocoder.osm(input_value)
  if not g.x or not g.y:
    # Set defaults to Empire State Building
    lon = -73.984892
    lat = 40.748121
  else:
    lon = g.x
    lat = g.y
  return "Your location: "+input_value #lon, lat


@app.callback(Output('graph', 'figure'),
              [Input('interval-component', 'n_intervals'),Input(component_id='my-div', component_property='children')])
def make_figure(n,coord):
  # lon = coord[0]
  # lat = coord[1]
  print(str(lat)+", "+str(lon))
  try:
    start = time.time()
    connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                  password = os.getenv("DB_PW"),
                                  host = os.getenv("DB_SERVER"),
                                  port = os.getenv("DB_PORT"),
                                  database = os.getenv("DB_NAME"))

    cursor = connection.cursor()

    lendf=0
    multi=1
    while( lendf==0):
      now = datetime.utcnow()
      five_minutes_ago = now - timedelta(hours=0, minutes=0, seconds=10)

      radius=500*multi
      # create_table_query = '''SELECT * FROM ride_share_data WHERE ST_DWithin(geom_end, ST_GeographyFromText('SRID=4326;POINT(  %s %s  )'), %s) AND Process_time < '%s' AND Process_time > '%s'; '''%(lon, lat, radius, now, five_minutes_ago)
      create_table_query = '''SELECT * FROM ride_share_data ORDER BY Process_time DESC FETCH FIRST 15 ROWS ONLY '''

      df = pd.read_sql_query(create_table_query, connection)

      lendf=len(df)
      multi=2
      if radius>4000:
        df = pd.DataFrame([[0,0,0,0]],columns=["End_lat","End_Lon","vendor_name","fare_amt"])
        break

    px.set_mapbox_access_token(token)

    # if df.vendor_name:
    lats_lyft = df[ (df["vendor_name"].str.contains("CMT")) | (df["vendor_name"].str.contains("1")) ]["end_lat"]
    lons_lyft = df[ (df["vendor_name"].str.contains("CMT")) | (df["vendor_name"].str.contains("1")) ]["end_lon"]

    lats_uber = df[ (df["vendor_name"].str.contains("VTS")) | (df["vendor_name"].str.contains("2")) ]["end_lat"]
    lons_uber = df[ (df["vendor_name"].str.contains("VTS")) | (df["vendor_name"].str.contains("2")) ]["end_lon"]

    lats_citibike = df[ df["vendor_name"].str.contains("Citi") ]["end_lat"]
    lons_citibike = df[ df["vendor_name"].str.contains("Citi") ]["end_lon"]

    data = [
      go.Scattermapbox(
      lat=lats_citibike,
      lon=lons_citibike,
      mode='markers', name='Citi Bike', 
      marker={'color': 'Gray', 'size': 15, 'symbol': "bicycle"},
      text=["Citi Bike"],
      ), 

      go.Scattermapbox(
      lat=lats_uber,
      lon=lons_uber,
      mode='markers', name='Uber', 
      marker=dict(
                  color='black',
                  size=10
              ),
      text=["Uber"],
      ), 

      go.Scattermapbox(
      lat=lats_lyft,
      lon=lons_lyft,
      mode='markers', name='Lyft', 
      marker=dict(
                  color='Magenta',
                  size=10
              ),
      text=["Lyft"],
      ), 

      go.Scattermapbox(
      lat=[lat],
      lon=[lon],
      mode='markers', name='You are here', 
      marker=dict(
                  color='red',
                  size=10
              ),
      text=['You are here'],
      )
      ]

    layout = go.Layout(
      autosize=True,
      # width=1000,
      height=600, 
      mapbox=dict( accesstoken=token, center=dict( lat=lat, lon=lon ), zoom=13, style=os.getenv("MAPBOX_STYLE") ),
      margin=dict(
        l=35,
        r=35,
        b=35,
        t=45
        ),
      ) 
    end = time.time()
    # print("Time: "+str(end - start))        

    fig = go.Figure( data, layout)
    return fig
  except:
    pass
  else:
    return pd.DataFrame()
  


if __name__ == '__main__':
  lon = -73.984892
  lat = 40.748121
  
  app.run_server(debug=True, host='0.0.0.0')