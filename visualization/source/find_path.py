import folium
from folium.plugins import HeatMap
import pandas as pd
from app.__init__ import *
from datetime import datetime, date, timedelta

def find_path_and_save_map(departure_station, arrival_station,
 startDateTime=None,
 endDateTime=None,
 min_probability_of_sucess=0.95, heatmap_duration = 0):

    heatmap_data = None
    res = None

    if departure_station == arrival_station:
        return "Please try again. You selected identical departure and arrival points."

    if endDateTime != None:
        # If an end time is specified, we maximize the departure time
        res = plan(departure_station, arrival_station,
               end_datetime=endDateTime,
               min_probability_of_success=min_probability_of_sucess)
    else:
        #Minimizing the arrival time
        res = plan(departure_station, arrival_station, start_datetime=startDateTime,
        min_probability_of_success=min_probability_of_sucess)
        if heatmap_duration != 0:
            heatmap_data = plan(departure_station, start_datetime=startDateTime,
            min_probability_of_success=min_probability_of_sucess, heatmap = True, heatmap_duration=heatmap_duration)

    print('query result', res)

    zurich_map = get_map_with_plot(res, heatmap_data)
    zurich_map.save('./templates/zurich_map.html')

    if not res['path']:
        return "The shortest route is to walk in this case."
    else:
        return res['departure_time'] + ' ' + res['path'][0]['src'] + ' -> ' + res['arrival_time'] + ' ' + res['path'][-1]['dst'] + ' - Duration: ' +  res['duration']

def get_description(node):
    prefix = ''
    if node['type'] == 'walk':
        prefix = 'Walk: '
    else:
        prefix = node['type'] + ' ' + node['line'] + ': '
    return prefix + node['departure_time'] + ' ' + node['src'] + ' -> ' + node['arrival_time'] + ' ' + node['dst']

def get_map_with_plot(res, heatmap_data = None):
    m = zurich_map = folium.Map(location=[47.376846, 8.543938],
     zoom_control = True, min_zoom=10, zoom_start=11,
      tiles="cartodbpositron", width='75%', height='75%')

    stations = pd.read_pickle("../resources/stations.pkl")

    if heatmap_data != None:
        heatmap_list = []
        for key, value in heatmap_data.items():
            station = stations.loc[key]
            folium.Circle(
              location=[station['Latitude'], station['Longitude']],
              radius=value,
              color='green',
              fill=True,
              fill_color='green',
              fill_opacity=0.07,
              weight=0.1
           ).add_to(m)

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
