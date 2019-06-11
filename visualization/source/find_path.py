import folium
import pandas as pd
from . import __init__
import datetime as dt
import calendar
import time
import numpy as np
import scipy.stats as stat
import pyspark.sql.functions as functions
import math
import getpass
import pyspark
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession
import networkx as nx

def find_path_and_save_map(departure_station, arrival_station,
 startDateTime=None,
 endDateTime=None,
 min_probability_of_sucess=0.0):
    # Maximize departure time
    fromStation = 'Kilchberg'
    toStation   = 'Urdorf, Schlierenstrasse'
    endDateTime   = datetime(2019, 6, 4, 19, 57)

    res = plan(fromStation, toStation,
           end_datetime=endDateTime,
           min_probability_of_success=0.95)

    # Minimize arrival time
    #startDateTime = datetime(2017, 9, 13, 12, 20)

    #res = plan(fromStation, toStation, start_datetime=startDateTime, min_probability_of_success=0.95)

    # How far can we go in M minutes example
    #res = plan(fromStation, start_datetime=startDateTime, min_probability_of_success=0.95, heatmap=True, heatmap_duration=30)

    zurich_map = get_map_with_plot(res)
    zurich_map.save('./templates/zurich_map.html')
    return res['departure time'] + ' ' + res['path'][0]['src'] + ' -> ' + res['arrival_time'] + ' ' + res['path'][-1]['dst'] + ' - Duration: ' +  res['duration']

def get_description(node):
    prefix = ''
    if node['type'] == 'walk':
        prefix = 'Walk: '
    else:
        prefix = node['type'] + ' ' + node['line'] + ': '
    return prefix + node['departure_time'] + ' ' + node['src'] + ' -> ' + node['arrival_time'] + ' ' + node['dst']

def get_map_with_plot(res):
    m = zurich_map = folium.Map(location=[47.376846, 8.543938],
     zoom_control = False, min_zoom=11, max_zoom=11, zoom_start=11,
      tiles="cartodbpositron", width='75%', height='75%')
    stations = pd.read_pickle("../resources/stations.pkl")
    trip = res['path']
    for idx, node in enumerate(trip):
        src_station = stations.loc[node['src']]
        src_pos = (src_station['Latitude'], src_station['Longitude'])
        dst_station = stations.loc[node['dst']]
        dst_pos = (dst_station['Latitude'], dst_station['Longitude'])

        description = get_description(node)

        if idx == 0:
            # Start station
            folium.vector_layers.Marker(src_pos, popup=description, tooltip=src_station.name, icon=folium.Icon(color='green')).add_to(m)
        else:
            folium.vector_layers.Marker(src_pos, popup=description, tooltip=src_station.name).add_to(m)

        folium.PolyLine([src_pos, dst_pos], color="blue", weight=2.5, opacity=1).add_to(m)

        if idx == (len(trip) - 1):
            # Plot arrival point
            folium.vector_layers.Marker(dst_pos, popup=None, tooltip=dst_station.name, icon=folium.Icon(color='red')).add_to(m)

    return m
