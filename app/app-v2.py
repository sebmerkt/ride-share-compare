#!/bin/python3

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

import pandas as pd
from numpy import round,arange

import psycopg2
from psycopg2 import Error

import plotly.express as px
import plotly.graph_objects as go

import os
from datetime import datetime, timedelta
import geocoder
import time
import json


# Choose colors
dark_mode=False

if dark_mode:
  bkg = '#333432'
else:
  bkg = '#e6e6e6'

if dark_mode:
  map_mode = "dark"
else:
  map_mode="light"


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
    __name__, external_stylesheets=["/assets/bootstrap.css"]
)

# Define colors
colors = { 'plotly_blue': '#119dff',
           'UserLocation': '#E6E600',
           'Lyft': 'Magenta',
           'Uber': 'white',
           'CitiBike': '#0066ff',
           'background': bkg
}


# Define the layout
app.layout = html.Div(
  [ 
    # Title
    html.H1(" Ride-Share-Compare ",
            style={ 'textAlign': 'center',
                    'color': colors['plotly_blue'],
                   "background": colors['background']},),

    # Input field for address search
    html.P([
      html.B("Enter your pickup location:  "),
        dcc.Input(id='my-id', value='11 Wall Street, New York', type='text', style={ 'textAlign': 'left',
                    'color': colors['plotly_blue'], "background": colors['background']}),
        html.Button('Update', id='button', style={'color': colors['plotly_blue']}),
        ],
        style={ 'textAlign': 'left',
                    'color': colors['plotly_blue'],
               "background": colors['background']},),

    # Automatically refresh map to get up-to-date ride data
    dcc.Interval(
      id='interval-component',
      interval=5*1000, # in milliseconds
      n_intervals=0
    ),

    # Page layout
    html.Div(className='row', children=[

      # Information about individual rides
      html.Div([
        dcc.Markdown("""
            **Ride information**
        """),
        html.Pre(id='click-data', style={'color': colors['plotly_blue']}),
      ],  style={'width': '25%', 'display': 'inline-block', 'vertical-align': 'top'}),

      # Show the map
      html.Div([
        dcc.Graph(id="graph", config={'displayModeBar': False}, style={"width": "100%", "display": "inline-block"}),
      ], style={'width': '75%', 'display': 'inline-block'}),
    ],
    style={ "background": colors['background'], 'color': colors['plotly_blue'],},
    ),
  ],style={ "background": "#ffffff"},
)


# Define data of individual rides
@app.callback(
    Output('click-data', 'children'),
    [Input('graph', 'clickData')])
def display_click_data(clickData):
  
  # Check if clickdata is empty
  if clickData:
    try:
      # Decide if Bike of car
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

        # Decide if Lyft or Uber
        if "CMT" in clickData["points"][0]["customdata"][3] or "1" in clickData["points"][0]["customdata"][3]:
          ride_type = "Lyft"
        else:
          ride_type = "Uber"
        
        # Return the ride info
        ret+=" Ride type: %s"%( ride_type )
        
        ret+="\n Expected fare per km: %s "%( fare_per_dist )
        
        ret+="\n Distance from your location: %s km"%( round( clickData["points"][0]["customdata"][2]/1000, decimals=2 ) )
        
        return ret
      else:
        return " Ride type: Citi Bike\n Distance from your location: %s km"%( round( clickData["points"][0]["customdata"][2]/1000, decimals=2 ) )
    except:
      # If data is not accessible, do nothing
      return " Please select a ride"
  else:
    # If ride data does not exist, do nothing
    return " Please select a ride"


# Draw the map if 1) refresh signal received 2) New user position is entered
@app.callback(Output('graph', 'figure'),
              [Input('interval-component', 'n_intervals'),Input('button', 'n_clicks')],state=[State(component_id='my-id', component_property='value')])
def make_figure(n_interval, n_clicks, input_value):
  
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
    largest_distance=0
    # If no rides found, extend radius and keep looking
  
    now = datetime.utcnow()
    some_time_ago = now - timedelta(hours=0, minutes=0, seconds=20)

    # Static query for testing
    # create_table_query = '''SELECT vendor_name, total_amt, trip_distance, end_lon, end_lat, dolocationid, ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography)
    # FROM ride_share_data ORDER BY ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography) ASC FETCH FIRST 10 ROWS ONLY;'''%(lon, lat, lon, lat)

    # Streaming query
    create_table_query = '''SELECT vendor_name, total_amt, trip_distance, end_lon, end_lat, dolocationid, ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography)
    FROM ride_share_data  WHERE Process_time < '%s' AND Process_time > '%s' ORDER BY ST_Distance(geom_end::geography, 'SRID=4326;POINT( %s %s )'::geography) ASC FETCH FIRST 15 ROWS ONLY;'''%(lon, lat, now, some_time_ago, lon, lat)

    # fetch data from PostGIS and save in pandas dataframe
    df = pd.read_sql_query(create_table_query, connection)

    largest_distance=df.st_distance.max()
    
    # Save number of rides found
    lendf=len(df)
    # Adjust zoom level to distance of the rides to the user location
    if largest_distance<=1200:
      zoomlevel = 14
    elif largest_distance<=2000 and largest_distance>1200:
      zoomlevel = 13
    elif largest_distance<=10000 and largest_distance>2000:
      zoomlevel = 12
    elif largest_distance<=15000 and largest_distance>10000:
      zoomlevel = 11
    elif largest_distance<=20000 and largest_distance>15000:
      zoomlevel = 10
    else:
      zoomlevel = 9
    print("HERE")
    
    # Import mapbox token
    px.set_mapbox_access_token(token)

    # Assign the data to each ride-share provider according to the vendor name:
    lyft_data = df[ (df["vendor_name"].str.contains("CMT")) | (df["vendor_name"].str.contains("1")) ]
    df_lyft_new = lyft_data[lyft_data.dolocationid.astype("float")>0]
    df_lyft_old = lyft_data[(lyft_data.dolocationid.isna()) | (lyft_data.dolocationid.astype("float")==0)]
    lats_lyft = df_lyft_old["end_lat"]
    lons_lyft = df_lyft_old["end_lon"]

    uber_data = df[ (df["vendor_name"].str.contains("VTS")) | (df["vendor_name"].str.contains("2")) ]
    df_uber_new = uber_data[uber_data.dolocationid.astype("float")>0]
    df_uber_old = uber_data[(uber_data.dolocationid.isna()) | (uber_data.dolocationid.astype("float")==0)]
    lats_uber = df_uber_old["end_lat"]
    lons_uber = df_uber_old["end_lon"]

    citibike_data = df[ df["vendor_name"].str.contains("Citi") ]
    lats_citibike = citibike_data["end_lat"]
    lons_citibike = citibike_data["end_lon"]

    # Get the number of rides per location ID
    df_loc=df_lyft_new.append(df_uber_new)
    rides_per_loc=df_loc.groupby("dolocationid")["dolocationid"].transform("count")


  except:
    # If fetching data failed, do nothing
    pass

  if lendf>0:
    # Define color scale for the ride locations IDs
    if len(rides_per_loc.unique())>1:
      color_range=['rgba(%s,%s,%s,0.3)'%(int(i/max(rides_per_loc)*200), int(i/max(rides_per_loc)*255), int(100+i/max(rides_per_loc)*155)) for i in list(reversed(sorted(rides_per_loc.unique())))]
    else:
      color_range=['rgba(200,255,255,0.3)','rgba(100,127,177,0.3)']

    # Define the data
    data = [
      go.Choroplethmapbox(geojson=city_locations, colorscale=color_range,
                          z=rides_per_loc,
                          locations=df_loc.dolocationid, featureidkey="properties.LocationID",
                          hovertemplate = ['%s rides in neighborhood'%i if i>1 else '%s ride in neighborhood'%i for i in rides_per_loc],
                          text=["Rides"],
                          name='',
                          showscale=False,
                          ),

      go.Scattermapbox(
      lat=lats_citibike,
      lon=lons_citibike,
      mode='markers', name='Citi Bike', 
      marker={'color': colors['CitiBike'], 'size': 12, 'symbol': "bicycle"},
      line_color="lightskyblue",
      hovertemplate = ['Citi Bike' for i in range(len(lons_citibike))],
      customdata=citibike_data[["total_amt", "trip_distance", "st_distance", "vendor_name"]],
      text=["Citi Bike"],
      # showlegend=False,
      ), 

      go.Scattermapbox(
      lat=lats_uber,
      lon=lons_uber,
      mode='markers', name='Uber', 
      marker=go.scattermapbox.Marker(
            size=11,
            color=colors['Uber'],
            opacity=1
        ),
      hovertemplate = ['Uber' for i in range(len(lons_uber))],
      customdata=uber_data[["total_amt", "trip_distance", "st_distance", "vendor_name"]],
      text=["Uber"],
      # showlegend=False,
      ), 

      go.Scattermapbox(
      lat=lats_lyft,
      lon=lons_lyft,
      mode='markers', name='Lyft', 
      marker=go.scattermapbox.Marker(
            size=11,
            color=colors['Lyft'],
            opacity=1
        ),
      hovertemplate = ['Lyft' for i in range(len(lons_lyft))],
      customdata=lyft_data[["total_amt", "trip_distance", "st_distance", "vendor_name"]],
      text=["Lyft"],
      # showlegend=False,
      ),

      go.Scattermapbox(
      lat=[lat],
      lon=[lon],
      mode='markers', name='You are here', 
      marker=go.scattermapbox.Marker(
            size=11,
            color=colors['UserLocation'],
            opacity=1
        ),
      hovertemplate = [input_value],
      # showlegend=False,
      ),

    ]
  else:
    data = [
      go.Scattermapbox(
      lat=[lat],
      lon=[lon],
      mode='markers', name='You are here', 
      marker=go.scattermapbox.Marker(
            size=11,
            color=colors['UserLocation'],
            opacity=1
        ),
      hovertemplate = [input_value],
      # showlegend=False,
      ),

    ]
  
  # Define map layout
  layout = go.Layout(
    autosize=True,
    # width=1000,
    height=800, 
    # Center around user position
    mapbox=dict( accesstoken=token,
                  center=dict( lat=lat, lon=lon ),
                  zoom=zoomlevel,
                #  style=os.getenv("MAPBOX_STYLE") ),
                #  style="streets" ),
                  style=map_mode ),
    margin=dict(
        l=5,
        r=5,
        b=5,
        t=5
      ),
    clickmode='event',
    hovermode='closest',
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(
      size=18,
      color=colors['plotly_blue']
  )
  )

  config={'displayModeBar': True}

  # Return the map
  fig = go.Figure( data, layout)
  return fig

  

# Start the Dash app
if __name__ == '__main__':
  app.run_server(debug=True, host='0.0.0.0')