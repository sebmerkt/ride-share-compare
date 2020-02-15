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
from numpy import round

import plotly.graph_objects as go



# get the mapbox token
token = open(os.getenv("MAPBOX_TOKEN")).read()

# connect to database
try:
  connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                password = os.getenv("DB_PW"),
                                host = os.getenv("DB_SERVER"),
                                port = os.getenv("DB_PORT"),
                                database = os.getenv("DB_NAME"))
except (Exception, psycopg2.DatabaseError) as error:
    print(error)


app = dash.Dash(
    __name__, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"]
)

styles = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll'
    }
}

app.layout = html.Div(
    [
        html.H1("Ride-Share-Compare"),
        html.P([
          html.B("Enter your pickup location:  "),
            dcc.Input(id='my-id', value='11 Wall Street, New York', type='text'),
            ]),
        dcc.Interval(
            id='interval-component',
            interval=3*1000, # in milliseconds
            n_intervals=0
        ),
        html.Div(className='row', children=[
          html.Div([
              dcc.Markdown("""
                  **Select a ride for more information**
              """),
              html.Pre(id='click-data'),
          ],  style={'width': '30%', 'display': 'inline-block'}),
          html.Div([
            dcc.Graph(id="graph", style={"width": "100%", "display": "inline-block"}),
          ],  style={'width': '70%', 'display': 'inline-block'}),
        ]),
    ]
)

# import json
@app.callback(
    Output('click-data', 'children'),
    [Input('graph', 'clickData')])
def display_click_data(clickData):
  ret = ""
  if clickData:
    try:
      if clickData["points"][0]["customdata"][1] >0:
        fare_per_dist = round( clickData["points"][0]["customdata"][0]/clickData["points"][0]["customdata"][1], decimals=2 )
      else:
        fare_per_dist = "not available"
      
      return '''Expected fare per mile: $ %s '''%( fare_per_dist )
    except:
      return "Please select a ride"
  else:
    return "Please select a ride"


@app.callback(Output('graph', 'figure'),
              [Input('interval-component', 'n_intervals'),Input(component_id='my-id', component_property='value')])
def make_figure(n,input_value):
  
  g = geocoder.osm(input_value)
  if not g.x or not g.y:
    # Set defaults to Empire State Building
    lon = -73.984892
    lat = 40.748121
  else:
    lon = g.x
    lat = g.y
    
  try:
    start = time.time()

    # cursor = connection.cursor()

    lendf=0
    multi=1
    while( lendf==0):
      now = datetime.utcnow()
      five_minutes_ago = now - timedelta(hours=0, minutes=0, seconds=10)

      radius=500*multi
      # create_table_query = '''SELECT * FROM ride_share_data WHERE ST_DWithin(geom_end, ST_GeographyFromText('SRID=4326;POINT(  %s %s  )'), %s) AND Process_time < '%s' AND Process_time > '%s'; '''%(lon, lat, radius, now, five_minutes_ago)
      create_table_query = '''SELECT * FROM ride_share_data WHERE ST_DWithin(geom_end, ST_GeographyFromText('SRID=4326;POINT( %s %s  )'), %s)  FETCH FIRST 15 ROWS ONLY'''%(lon, lat, radius)

      df = pd.read_sql_query(create_table_query, connection)

      lendf=len(df)
      multi=2
      if radius>4000:
        df = pd.DataFrame([[0,0,0,0]],columns=["End_lat","End_Lon","vendor_name","fare_amt"])
        break

    px.set_mapbox_access_token(token)

    # if df.vendor_name:
    lyft_data = df[ (df["vendor_name"].str.contains("CMT")) | (df["vendor_name"].str.contains("1")) ]
    lats_lyft = lyft_data["end_lat"]
    lons_lyft = lyft_data["end_lon"]

    uber_data = df[ (df["vendor_name"].str.contains("VTS")) | (df["vendor_name"].str.contains("2")) ]
    lats_uber = uber_data["end_lat"]
    lons_uber = uber_data["end_lon"]

    citibike_data = df[ df["vendor_name"].str.contains("Citi") ]
    lats_citibike = citibike_data["end_lat"]
    lons_citibike = citibike_data["end_lon"]

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
      hoverinfo="none",
      customdata=df[["total_amt","trip_distance"]],
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
      hoverinfo="none",
      customdata=df[["total_amt","trip_distance"]],
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
      hoverinfo="none",
      )
      ]

    layout = go.Layout(
      autosize=True,
      # width=1000,
      height=450, 
      mapbox=dict( accesstoken=token, center=dict( lat=lat, lon=lon ), zoom=13, style=os.getenv("MAPBOX_STYLE") ),
      margin=dict(
        l=15,
        r=15,
        b=15,
        t=20
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

def get_fare_per_distance(fare,dist):
  # try:
    fare_per_dist=fare.astype('float')/dist.astype('float')
    # if dist>0:
    print(fare_per_dist)
    return fare_per_dist.to_frame()
    # else:
    #   return "No expected fare available"
    # except:
    #   return "No expected fare available"
  


if __name__ == '__main__':
  app.run_server(debug=True, host='0.0.0.0')