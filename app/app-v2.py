#!/bin/python3

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import pandas as pd
from numpy import round

import psycopg2
from psycopg2 import Error

import plotly.express as px
import plotly.graph_objects as go

import os
from datetime import datetime, timedelta
import geocoder
import time
import json


# Import taxi zones
with open('./taxi_zones.geojson') as zones:
    city_locations = json.load(zones)



# Import the mapbox token
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

# Initialize app
app = dash.Dash(
    __name__, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"]
)

# Define the layout
app.layout = html.Div(
  [ 
    # Title
    html.H1(dcc.Markdown(" **Ride-Share-Compare** ")),

    # Input field for address search
    html.P([
      html.B("Enter your pickup location:  "),
        dcc.Input(id='my-id', value='11 Wall Street, New York', type='text'),
        ]),

    # Automatically refresh map to get up-to-date ride data
    dcc.Interval(
      id='interval-component',
      interval=30*1000, # in milliseconds
      n_intervals=0
    ),

    # Page layout
    html.Div(className='row', children=[

      # Information about individual rides
      html.Div([
        dcc.Markdown("""
            **Ride information**
        """),
        html.Pre(id='click-data'),
      ],  style={'width': '25%', 'display': 'inline-block', 'vertical-align': 'top'}),

      # Show the map
      html.Div([
        dcc.Graph(id="graph", style={"width": "100%", "display": "inline-block"}),
      ], style={'width': '75%', 'display': 'inline-block'}),
    ]),
  ]
)

# Define data of individual rides
@app.callback(
    Output('click-data', 'children'),
    [Input('graph', 'clickData')])
def display_click_data(clickData):
  
  # Check if clickdata is empty
  if clickData:
    try:
      if not "Citi" in clickData["points"][0]["customdata"][3]:
        # Create output string
        ret = "" 

        # Check if trip distance is greater zero
        if clickData["points"][0]["customdata"][1] >0 and clickData["points"][0]["customdata"][0]>0:
          # calculate ride fare per distance
          fare_per_dist = "$ "+str( round( clickData["points"][0]["customdata"][0]/clickData["points"][0]["customdata"][1], decimals=2 ) )
        else:
          # If distance is zero, not value can be displayed
          fare_per_dist = "not available"

        if "CMT" in clickData["points"][0]["customdata"][3] or "1" in clickData["points"][0]["customdata"][3]:
          ride_type = "Lyft"
        else:
          ride_type = "Uber"
        
        # Return the ride info
        ret+="Ride type: %s"%( ride_type )
        
        ret+="\nExpected fare per km: %s "%( fare_per_dist )
        
        ret+="\nDistance from your location: %s km"%( round( clickData["points"][0]["customdata"][2]/1000, decimals=2 ) )
        
        return ret
      else:
        return "Ride type: Citi Bike\nDistance from your location: %s km"%( round( clickData["points"][0]["customdata"][2]/1000, decimals=2 ) )
    except:
      # If data is not accessible, do nothing
      return "Please select a ride"
  else:
    # If ride data does not exist, do nothing
    return "Please select a ride"


# Draw the map if 1) refresh signal received 2) New user position is entered
@app.callback(Output('graph', 'figure'),
              [Input('interval-component', 'n_intervals'),Input(component_id='my-id', component_property='value')])
def make_figure(n,input_value):
  
  # Translate address to geographical coordinates
  g = geocoder.osm(input_value)
  if not g.x or not g.y:
    # If no address is found, set defaults to Empire State Building
    lon = -73.984892
    lat = 40.748121
  else:
    # return geocoded location
    lon = g.x
    lat = g.y
    
  # Retreive data from PostGIS
  try:
    # Define number of rides found and a multiplication factor to extend search radius if necessary
    lendf=0
    multi=1
    # If no rides found, extend radius and keep looking
    while( lendf==0):
      # Save time window between now and window start
      now = datetime.utcnow()
      some_time_ago = now - timedelta(hours=0, minutes=0, seconds=20)

      # Extend search radius
      radius=500*multi
      # Static query
      # create_table_query = '''SELECT vendor_name, total_amt, trip_distance, end_lon, end_lat, dolocationid, ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography)
      # FROM ride_share_data ORDER BY ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography) ASC FETCH FIRST 10 ROWS ONLY;'''%(lon, lat, lon, lat)

      # Streaming query
      create_table_query = '''SELECT vendor_name, total_amt, trip_distance, end_lon, end_lat, dolocationid, ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography)
      FROM ride_share_data  WHERE Process_time < '%s' AND Process_time > '%s' ORDER BY ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography) ASC FETCH FIRST 10 ROWS ONLY;'''%(lon, lat, now, some_time_ago, lon, lat)

      # fetch data from PostGIS and save in pandas dataframe
      df = pd.read_sql_query(create_table_query, connection)
      
      # Save number of rides found
      lendf=len(df)
      
      # Increase multiplication factor to increase search radius
      multi=2
      if multi==4:
        df=pd.DataFrame(columns=["vendor_name", "total_amt", "trip_distance", "end_lon", "end_lat", "dolocationid", "st_distance"])

    # Import mapbox token
    px.set_mapbox_access_token(token)

    print(df)

    # Assign the data to each ride-share provider according to the vendor name:
    lyft_data = df[ (df["vendor_name"].str.contains("CMT")) | (df["vendor_name"].str.contains("1")) ]
    df_lyft_new = lyft_data[lyft_data.dolocationid.astype("float")>0]
    df_lyft_old = lyft_data[(lyft_data.dolocationid.isna()) | (lyft_data.dolocationid.astype("float")==0)]
    lats_lyft = df_lyft_old["end_lat"]
    lons_lyft = df_lyft_old["end_lon"]

    print("lyft_data")
    print(lyft_data)
    print("df_lyft_new")
    print(df_lyft_new)
    print("df_lyft_old")
    print(df_lyft_old)

    uber_data = df[ (df["vendor_name"].str.contains("VTS")) | (df["vendor_name"].str.contains("2")) ]
    print("HERE1")
    df_uber_new = uber_data[uber_data.dolocationid.astype("float")>0]
    print("HERE2")
    df_uber_old = uber_data[(uber_data.dolocationid.isna()) | (uber_data.dolocationid.astype("float")==0)]
    print("HERE3")
    lats_uber = df_uber_old["end_lat"]
    print("HERE4")
    lons_uber = df_uber_old["end_lon"]
    print("HERE5")


    citibike_data = df[ df["vendor_name"].str.contains("Citi") ]
    lats_citibike = citibike_data["end_lat"]
    lons_citibike = citibike_data["end_lon"]




    # Define the data
    data = [
      go.Scattermapbox(
      lat=lats_citibike,
      lon=lons_citibike,
      mode='markers', name='Citi Bike', 
      marker={'color': 'Gray', 'size': 10, 'symbol': "bicycle"}, #bicycle-share-15, bicycle-11, bicycle-15
      hovertemplate = ['Citi Bike' for i in range(len(lons_citibike))],
      customdata=citibike_data[["total_amt", "trip_distance", "st_distance", "vendor_name"]],
      text=["Citi Bike"],
      ), 

      go.Scattermapbox(
      lat=lats_uber,
      lon=lons_uber,
      mode='markers', name='Uber', 
      marker=go.scattermapbox.Marker(
            size=10,
            color='black',
            opacity=1
        ),
      hovertemplate = ['Uber' for i in range(len(lons_uber))],
      customdata=uber_data[["total_amt", "trip_distance", "st_distance", "vendor_name"]],
      text=["Uber"],
      ), 

      go.Scattermapbox(
      lat=lats_lyft,
      lon=lons_lyft,
      mode='markers', name='Lyft', 
      marker=go.scattermapbox.Marker(
            size=10,
            color='Magenta',
            opacity=1
        ),
      hovertemplate = ['Lyft' for i in range(len(lons_lyft))],
      customdata=lyft_data[["total_amt", "trip_distance", "st_distance", "vendor_name"]],
      text=["Lyft"],
      ),

      go.Scattermapbox(
      lat=[lat],
      lon=[lon],
      mode='markers', name='You are here', 
      marker=go.scattermapbox.Marker(
            size=10,
            color='red',
            opacity=1
        ),
      hovertemplate = [input_value],
      ),

      go.choroplethmapbox(geojson=city_locations, colorscale="Viridis", z=df_lyft_new.total_amt,#df_lyft_new.groupby("dolocationid")["dolocationid"].transform("count"),
                          locations=df_lyft_new.dolocationid, featureidkey="properties.LocationID"#,
                          #  center={"lat": lat, "lon": lon},
                          #  mapbox_style="carto-positron", zoom=9)
                          ),
      # go.Choroplethmapbox(geojson=africa_geo, locations=data.country_name, z=data.ranking,
      #                               colorscale="Viridis"))
      # go.choroplethmapbox(df_uber_new, geojson=locations, color=df_uber_new.groupby("DOLocationID")["DOLocationID"].transform("count"),
      #                      locations="DOLocationID", featureidkey="properties.LocationID"
      #                     ),
      ]

    # Define map layout
    layout = go.Layout(
      autosize=True,
      # width=1000,
      # height=450, 
      # Center around user position
      mapbox=dict( accesstoken=token, center=dict( lat=lat, lon=lon ), zoom=13, style=os.getenv("MAPBOX_STYLE") ),
      margin=dict(
          l=15,
          r=15,
          b=15,
          t=20
        ),
      clickmode='event',
      hovermode='closest'
    )

    # Return the map
    fig = go.Figure( data, layout)
    return fig
  except:
    # If fetching data failed, do nothing
    pass

  

# Start the Dash app
if __name__ == '__main__':
  app.run_server(debug=True, host='0.0.0.0')