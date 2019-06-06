import folium
import pandas as pd

def find_path_and_save_map(departure_station, arrival_station,
 startDateTime=None,
 endDateTime=None,
 min_probability_of_sucess=0.0):

    # TODO replace with real algo
    start_node = {'src': 'Kilchberg',
              'dst': 'Wallisellen',
              'type': 'Zug',
              'line': 'S14',
              'departure_day': 'Monday',
              'departure_time': '0-19:39:00',
              'arrival_time': '0-20:29:00',
              'lateAvg': 0,
              'lateStd': 0}
    mid_node = {'src': 'Wallisellen',
              'dst': 'Urdorf',
              'type': 'Zug',
              'line': 'S8',
              'departure_day': 'Monday',
              'departure_time': '0-20:07:00',
              'arrival_time': '0-20:29:00',
              'lateAvg': 0,
              'lateStd': 0}
    end_node = {'src': 'Urdorf',
              'dst': 'Urdorf, Schlierenstrasse',
              'type': 'walk',
              'line': 'walk',
              'departure_day': 'null',
              'departure_time': 'null',
              'arrival_time': 'null',
              'lateAvg': 626.844,
              'lateStd': 0}
    trip = [start_node, mid_node,end_node]

    zurich_map = get_map_with_plot(trip)
    zurich_map.save('./templates/zurich_map.html')


def get_map_with_plot(trip):
    m = zurich_map = folium.Map(location=[47.376846, 8.543938],
     zoom_control = False, min_zoom=11, max_zoom=11, zoom_start=11,
      tiles="cartodbpositron", width='75%', height='75%')
    stations = pd.read_pickle("../resources/stations.pkl")

    for idx, node in enumerate(trip):
        src_station = stations.loc[node['src']]
        src_pos = (src_station['Latitude'], src_station['Longitude'])
        dst_station = stations.loc[node['dst']]
        dst_pos = (dst_station['Latitude'], dst_station['Longitude'])

        if idx == 0:
            # Start station
            folium.vector_layers.Marker(src_pos, popup=None, tooltip=src_station.name).add_to(m)
        else:
            folium.vector_layers.Marker(src_pos, popup=None, tooltip=src_station.name).add_to(m)

        folium.PolyLine([src_pos, dst_pos], color="red", weight=2.5, opacity=1).add_to(m)

        if idx == (len(trip) - 1):
            # Plot arrival point
            folium.vector_layers.Marker(dst_pos, popup=None, tooltip=dst_station.name, icon=folium.Icon(color='red')).add_to(m)

    return m
