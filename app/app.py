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
  return lon, lat


@app.callback(Output('graph', 'figure'),
              [Input('interval-component', 'n_intervals'),Input(component_id='my-div', component_property='children')])
def make_figure(n,coord):
  # if not lon:
  lon = coord[0]
  # if not lat:
  lat = coord[1]
  # lon, lat = get_current_location()
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
    while( lendf<12):
      now = datetime.utcnow()
      five_minutes_ago = now - timedelta(hours=0, minutes=2, seconds=0)

      radius=100*multi
      create_table_query = '''SELECT * FROM ride_share_data WHERE ST_DWithin(geom_start, ST_GeographyFromText('SRID=4326;POINT(  %s %s  )'), %s) AND Process_time < '%s' AND Process_time > '%s'; '''%(lon, lat, radius, now, five_minutes_ago)
      # create_table_query = '''SELECT * FROM ride_share_data WHERE ST_DWithin(geom_start, ST_GeographyFromText('SRID=4326;POINT(  %s %s  )'), %s); '''%(lon, lat, radius)
      # create_table_query = '''SELECT * FROM ride_share_data ORDER BY Process_time DESC FETCH FIRST 15 ROWS ONLY '''

      df = pd.read_sql_query(create_table_query, connection)

      lendf=len(df)
      multi=2
      # if radius>5000:
      #   df = pd.DataFrame([[0,0,0,0]],columns=["End_lat","End_Lon","vendor_name","fare_amt"])
      #   break

    px.set_mapbox_access_token(token)

    # if df.vendor_name:
    lats1 = df.head(6)
    lons1 = df.head(6)
    lats2 = df.tail(6)
    lons2 = df.tail(6)

    data = [
      go.Scattermapbox(
      lat=lats1.end_lat,
      lon=lons1.end_lon,
      mode='markers', name='Lyft', 
      marker=dict(
                  color='Magenta',
                  size=10
              ),
      text=lats1.vendor_name,
      ), 
      go.Scattermapbox(
      lat=lats2.end_lat,
      lon=lons2.end_lon,
      mode='markers', name='Uber', 
      marker=dict(
                  color='black',
                  size=10
              ),
      text=lats2.vendor_name,
      ), 
      go.Scattermapbox(
      lat=[lat],
      lon=[lon],
      mode='markers', name='You are here', 
      # marker=dict(
      #             color='red',
      #             size=10
      #         ),
      marker={'size': 10, 'symbol': "car"}
      text=['You are here'],
      )
      ]

    layout = go.Layout(
      autosize=True,
      # width=1000,
      height=600, 
      mapbox=dict( accesstoken=token, center=dict( lat=lat, lon=lon ), zoom=12, style=os.getenv("MAPBOX_STYLE") ),
      margin=dict(
        l=35,
        r=35,
        b=35,
        t=45
        ),
      ) 
    end = time.time()
    print("Time: "+str(end - start))        

    fig = go.Figure( data, layout)
    return fig
  except:
    pass
  else:
    return pd.DataFrame()
  


if __name__ == '__main__':
  print("START")
  app.run_server(debug=True, host='0.0.0.0')