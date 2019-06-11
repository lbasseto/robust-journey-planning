from flask import Flask
from . import routes

import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as stat
import math
import networkx as nx

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
import getpass
from datetime import datetime, date, timedelta

spark = SparkSession \
    .builder \
    .master("yarn") \
    .appName('journey_planner-{0}'.format(getpass.getuser())) \
    .config('spark.jars.packages', 'graphframes:graphframes:0.6.0-spark2.3-s_2.11') \
    .config('spark.executor.memory', '8g') \
    .config('spark.executor.instances', '5') \
    .config('spark.port.maxRetries', '100') \
    .getOrCreate()

from graphframes import *

# Load data from hdfs
vertices = spark.read.parquet('/homes/schmutz/vertices')
edges = spark.read.parquet('/homes/schmutz/edges')
graph = GraphFrame(vertices, edges)

"""
Define some constants
"""
MINUTES_PER_DAY = 1440
MINUTES_PER_HOUR = 60
SECONDS_PER_MINUTE = 60

WALK_SPEED = 1000 * 5 / 60 # meters per minute


def compute_length_in_minutes_between(departure, arrival):
    """
    Computes the interval in minutes between the departure and arrival time
    """

    dep = (departure[2:]).split(':')
    arr = (arrival[2:]).split(':')
    day_minutes = (int(arrival[:1]) - int(departure[:1])) * MINUTES_PER_DAY
    hour_minutes = (int(arr[0]) - int(dep[0])) * MINUTES_PER_HOUR
    minutes = (int(arr[1]) - int(dep[1]))
    return day_minutes + hour_minutes + minutes


def compute_prob(arrival, late_avg, late_std, departure):
    """
    Computes the probability that we can use the departure time given that we arrive at the
    arrival time with a delay represented by a gaussian distribution with mean late_avg and
    standard deviation late_std
    """

    length = compute_length_in_minutes_between(arrival, departure) * SECONDS_PER_MINUTE
    if late_std != 0.0:
        return stat.norm(loc=late_avg, scale=late_std).cdf(length)
    elif late_avg <= length:
        return 1.0
    else:
        return 0.0


def compute_time_between(departure, arrival):
    """
    Computes the interval in hh:mm:ss between the departure and arrival time
    """

    dep = (departure[2:]).split(':')
    arr = (arrival[2:]).split(':')
    a = (int(arrival[:1]) - int(departure[:1])) * MINUTES_PER_DAY
    b = (int(arr[0]) - int(dep[0])) * MINUTES_PER_HOUR
    c = (int(arr[1]) - int(dep[1]))
    tot = a + b + c
    hours = tot // MINUTES_PER_HOUR
    minutes = tot % MINUTES_PER_HOUR
    return "{:02d}".format(int(hours)) + ':' + "{:02d}".format(int(minutes)) + ':00'


def time_op(start_time, duration, op='add'):
    """
    Add or subtract the given duration to or from start_time
    """

    tmp = start_time.split(':')
    a = int(tmp[0][2:]) * MINUTES_PER_HOUR + int(tmp[1])
    b = duration // SECONDS_PER_MINUTE
    if op == 'add':
        prefix = tmp[0][:2] if a + b < MINUTES_PER_DAY else '1-'
        a = (a + b) % MINUTES_PER_DAY
    elif op == 'sub':
        prefix = tmp[0][:2] if a > b else '0-'
        a = (a - b) % MINUTES_PER_DAY
    minutes = a % MINUTES_PER_HOUR
    hours = (a - minutes) // MINUTES_PER_HOUR
    return prefix + "{:02d}".format(int(hours)) + ':' + "{:02d}".format(int(minutes)) + ':00'


def add_time(start_time, duration):
    """
    Add the given duration to start_time
    """
    return time_op(start_time, duration, 'add')


def sub_time(start_time, duration):
    """
    Subtract the given duration from start_time
    """
    return time_op(start_time, duration, 'sub')


def get_filtered_edges(start_day, finish_day, start_time, finish_time, duration):
    """
    Filters the GraphFrames graph of vertices and edges and returns only the edges that can be used
    in a valid journey given the constraints
    """

    def valid(day, dep_time, arr_time, walk_time):
        if start_day==finish_day:
            return ((day=='null') & (walk_time<=duration)) | \
                    ((day==start_day) & (dep_time>=start_time) & (arr_time<=finish_time) & (dep_time<=arr_time))
        else:
            return ((day=='null') & (walk_time<=duration)) | \
                    (((day==start_day) & (dep_time>=start_time) & ((dep_time<=arr_time) | (arr_time<=finish_time))) | \
                     ((day==finish_day) & (dep_time<finish_time) & (arr_time<=finish_time)))

    return graph.filterEdges(valid(graph.edges.departure_day,
                                graph.edges.departure_time,
                                graph.edges.arrival_time,
                                graph.edges.lateAvg)).edges


def add_vertex_to_set(max_set, vertex, vertice_costs, subgraph, next_vertices, certain_path, reverse):

    max_set.add(vertex)
    cost = vertice_costs[vertex]

    if reverse:
        vertice_edges = subgraph.out_edges(vertex, data=True)

        for parallel_paths in vertice_edges:
            edge = parallel_paths[2]
            if edge['type'] == 'walk':
                new_cost = add_time(cost, edge['lateAvg'])
                if (vertex not in next_vertices or next_vertices[vertex]['type'] != 'walk') and \
                    (edge['dst'] not in vertice_costs or new_cost < vertice_costs[edge['dst']]):
                    next_vertices[edge['dst']] = edge
                    vertice_costs[edge['dst']] = new_cost
            elif edge['departure_time'] > cost and \
                (edge['dst'] not in vertice_costs or edge['arrival_time'] < vertice_costs[edge['dst']]):
                if (not certain_path) or vertex not in next_vertices or \
                    compute_prob(cost,  next_vertices[vertex]['lateAvg'],
                                next_vertices[vertex]['lateStd'], edge['departure_time']) == 1:
                    vertice_costs[edge['dst']] = edge['arrival_time']
                    next_vertices[edge['dst']] = edge
    else:
        vertice_edges = subgraph.in_edges(vertex, data=True)

        for parallel_paths in vertice_edges:
            edge = parallel_paths[2]
            if edge['type'] == 'walk':
                new_cost = sub_time(cost, edge['lateAvg'])
                if (vertex not in next_vertices or next_vertices[vertex]['type'] != 'walk') and \
                    (edge['src'] not in vertice_costs or new_cost > vertice_costs[edge['src']]):
                    next_vertices[edge['src']] = edge
                    vertice_costs[edge['src']] = new_cost
            elif edge['arrival_time'] < cost and \
                (edge['src'] not in vertice_costs or edge['departure_time'] > vertice_costs[edge['src']]):
                if (not certain_path) or compute_prob(edge['arrival_time'],  edge['lateAvg'], edge['lateStd'], cost) == 1:
                    vertice_costs[edge['src']] = edge['departure_time']
                    next_vertices[edge['src']] = edge


def get_max_vertex_not_in_set(max_set, vertice_costs, min_trip_departure_time):
    max_vertex = None
    max_cost = min_trip_departure_time
    for vertex in vertice_costs:
        if vertex not in max_set and vertice_costs[vertex] > max_cost:
            max_cost = vertice_costs[vertex]
            max_vertex = vertex

    return max_vertex


def get_min_vertex_not_in_set(max_set, vertice_costs, max_trip_arrival_time):
    max_vertex = None
    max_cost = max_trip_arrival_time
    for vertex in vertice_costs:
        if vertex not in max_set and vertice_costs[vertex] < max_cost:
            max_cost = vertice_costs[vertex]
            max_vertex = vertex

    return max_vertex


def find_path(next_vertices, current_vertex, current_path, direction):
    """
    Function only used for testing of find_shortest_path
    find_path(next_vertices, target, [target], direction) returns between the departure and arrival station
    the path is returned in reverse order if direction is set to src
    """

    if current_vertex not in next_vertices:
        return current_path
    next_vertex = next_vertices[current_vertex][direction]
    current_path.append(next_vertices[current_vertex])
    return find_path(next_vertices, next_vertex, current_path, direction)


def find_shortest_path(subgraph, departure_station, arrival_station,
                       min_trip_departure_time, max_trip_arrival_time, duration,
                       get_all_destinations=False, certain_path=False, reverse=False):

    """
    Uses Dijkstra's algorithm to find the shortest path between the departure station and the arrival station.
    The functions expects a subgraph of all valid edges in the context of the query.

    Returns the latest possible departure time in order to arrive to the destination in time.
    If get_all_destinations is set to True, it instead returns a dictionary mapping station names to latest possible
    departure times at that station in order to arrive to the destination in time.
    If certain_path is set to True, returns the latest possible departure time in order to arrive to the destination
    in time and with 100% probability.

    If reverse is set to True, then the algorithm is run in reverse order:
    Returns the earliest possible arrival time when leaving on or after the departure time.
    If get_all_destinations is set to True, it instead returns a dictionary mapping station names to earliest possible
    arrival times at that station if we left on of after the departure time.
    If certain_path is set to True, returns the earliest possible arrival time when leaving on or after the departure
    time and with 100% probability.
    """

    # as day#-hh-mm-ss
    vertice_costs = {}
    max_set = set()
    next_vertices = {}
    if reverse:
        vertice_costs[departure_station] = min_trip_departure_time
        target = arrival_station
        add_vertex_to_set(max_set, departure_station, vertice_costs, subgraph, next_vertices, certain_path, reverse)
        direction = 'src'
    else:
        vertice_costs[arrival_station] = max_trip_arrival_time
        target = departure_station
        add_vertex_to_set(max_set, arrival_station, vertice_costs, subgraph, next_vertices, certain_path, reverse)
        direction= 'dst'

    no_solution = False

    while((target not in max_set or get_all_destinations) and not no_solution):
        if reverse:
            min_vertex = get_min_vertex_not_in_set(max_set, vertice_costs, max_trip_arrival_time)
            if min_vertex is None:
                no_solution = True
            else:
                add_vertex_to_set(max_set, min_vertex, vertice_costs, subgraph, next_vertices, certain_path, reverse)
        else:
            max_vertex = get_max_vertex_not_in_set(max_set, vertice_costs, min_trip_departure_time)
            if max_vertex is None:
                no_solution = True
            else:
                add_vertex_to_set(max_set, max_vertex, vertice_costs, subgraph, next_vertices, certain_path, reverse)

    if get_all_destinations:
        return vertice_costs
    if no_solution:
        return 'no solution'

    if reverse:
        return vertice_costs[arrival_station]
    else:
        return vertice_costs[departure_station]


def compute_expected_arrival_time(arr_time, late_avg, late_std, curr_prob, min_prob_success):
    """
    Computes the upper bound of the arrival time meeting the uncertainty requirement given:
    arr_time the scheduled arrival time
    late_avg the expected delay
    late_std the standard deviation of the delay
    curr_prob the probability of catching the transport that got us here
    min_prob_success the constraint on the uncertainty requirement we have to respect
    """

    if late_std != 0:
        remaining_prob = min_prob_success / curr_prob
        expected_late = stat.norm(loc=late_avg, scale=late_std).ppf(remaining_prob)
        if expected_late < 0:
            return arr_time
        elif expected_late == np.inf:
            return None
        else:
            return add_time(arr_time, expected_late)
    else:
        return add_time(arr_time, late_avg)


def compute_paths_heatmap(src, subgraph, visited,
                          curr_prob, curr_time, curr_lateAvg, curr_lateStd,
                          min_trip_departure_time,
                          times, last_line_taken, time_limits, min_prob_success):
    """
    Explore the paths to obtain the heatmap
    src is the current location
    subgraph is the set of valid edges
    visited is a set of already visited stations in the current path
    curr_prob is the current probability of reaching this location
    curr_time is the time at which we arrived at this location according to the schedule
    curr_lateAvg is the delay we expect to add to curr_time to arrive at this location
    curr_lateStd is the standard deviation of the delay
    min_trip_departure time is the time when we began moving from the first station
    times is a dictionary of stations mapping to the earliest arrival time at that station (this function builds this map)
    last_line_taken is the line we took coming from the previous station
    time_limits is a dictionary mapping from stations to the earliest possible arrival time at that station with a probability
    of 100%
    min_pro_success is the constraint on the uncertainty requirement we have to respect
    """

    visited.add(src)

    arr_time = compute_expected_arrival_time(curr_time, curr_lateAvg, curr_lateStd, curr_prob, min_prob_success)

    if arr_time is not None and (src not in times or times[src] > arr_time):
        times[src] = arr_time

    vertice_edges = subgraph.out_edges(src, data=True)
    for vertice_edge in vertice_edges:
        edge = vertice_edge[2]

        if edge['dst'] not in visited and edge['line'] != last_line_taken:

            if edge['type'] == 'walk':
                new_time = add_time(curr_time, edge['lateAvg'])

                if edge['dst'] in time_limits and new_time <= time_limits[edge['dst']]:

                    compute_paths_heatmap(edge['dst'], subgraph, visited,
                                               curr_prob, new_time, curr_lateAvg, curr_lateStd,
                                               min_trip_departure_time, times,
                                               edge['line'], time_limits, min_prob_success)

            elif edge['departure_time'] > curr_time and edge['dst'] in time_limits and \
                 edge['arrival_time'] <= time_limits[edge['dst']]:

                prob = compute_prob(curr_time, curr_lateAvg, curr_lateStd, edge['departure_time'])
                new_prob = curr_prob * prob

                if new_prob >= min_prob_success:
                    compute_paths_heatmap(edge['dst'], subgraph, visited,
                                               new_prob, edge['arrival_time'], edge['lateAvg'], edge['lateStd'],
                                               min_trip_departure_time, times,
                                               edge['line'], time_limits, min_prob_success)

    visited.remove(src)


def compute_heatmap(subgraph, departure_station, arrival_station,
                    min_trip_departure_time, max_trip_arrival_time, duration,
                    min_probability_of_success):
    """
    Compute a heatmap
    subgraph is the set of valid edges in the context of the query
    departure_station is the station we want to start the heatmap from
    arrival_station is a dummy argument that we do not use
    min_trip_departure_time is the time when we can start moving
    max_trip_arrival_time is the time when we must stop moving
    duration is the difference between the two times
    min_probability of sucess
    """

    time_fastest = find_shortest_path(subgraph, departure_station, arrival_station,
                              min_trip_departure_time, max_trip_arrival_time, duration,
                              get_all_destinations=True, reverse=True, certain_path=False)

    time_limits = find_shortest_path(subgraph, departure_station, arrival_station,
                              min_trip_departure_time, max_trip_arrival_time, duration,
                              get_all_destinations=True, reverse=True, certain_path=True)

    for k, v in time_fastest.items():
        if k not in time_limits:
            time_limits[k] = max_trip_arrival_time


    visited = set()
    times = {}
    compute_paths_heatmap(departure_station, subgraph, visited, 1.0, min_trip_departure_time, 0.0, 0.0,
                          min_trip_departure_time, times, '', time_limits, min_probability_of_success)

    heat = {}
    for k, v in times.items():
        if v < max_trip_arrival_time:
            heat[k] = WALK_SPEED * compute_length_in_minutes_between(v, max_trip_arrival_time)

    return heat


def compute_dep_time(max_arr_time, curr_path, edge=None):
    """
    curr_path follows this format: [src, edge, edge, edge, ...] with the edges in normal order
    max_arr_time is only used when computing the departure time of a path with only one (walking) edge
    if edge is not None then it means we intend to append edge to the end of curr_path
    Returns the departure time
    """

    if len(curr_path) == 1:
        dep = max_arr_time if edge is None else edge['departure_time']
    elif curr_path[1]['type'] == 'walk':
        if edge is not None and len(curr_path) == 2:
            dep = sub_time(edge['departure_time'], curr_path[1]['lateAvg'])
        elif len(curr_path) > 2:
            dep = sub_time(curr_path[2]['departure_time'], curr_path[1]['lateAvg'])
        else:
            dep = sub_time(max_arr_time, curr_path[1]['lateAvg'])
    else:
        dep = curr_path[1]['departure_time']

    return dep


def compute_arr_time(min_dep_time, curr_path, edge=None):
    """
    curr_path follows this format: [dst, edge, edge, edge, ...] with the edges in reverse order
    min_dep_time is only used when computing the arrival time of a path with only one (walking) edge
    if edge is not None then it means we intend to append edge to the end of curr_path
    Returns the arrival time
    """

    if len(curr_path) == 1:
        arr = min_dep_time if edge is None else edge['arrival_time']
    elif curr_path[1]['type'] == 'walk':
        if edge is not None and len(curr_path) == 2:
            arr = add_time(edge['arrival_time'], curr_path[1]['lateAvg'])
        elif len(curr_path) > 2:
            arr = add_time(curr_path[2]['arrival_time'], curr_path[1]['lateAvg'])
        else:
            arr = add_time(min_dep_time, curr_path[1]['lateAvg'])
    else:
        arr = curr_path[1]['arrival_time']

    return arr


def compute_paths_arrival_mode(src, dst, subgraph, visited, curr_path,
                               curr_prob, curr_time, curr_lateAvg, curr_lateStd,
                               min_trip_departure_time, max_trip_arrival_time,
                               paths, last_line_taken, time_limits, min_prob_success, best_times):
    """
    Use the depth first search algorithm on the subgraph to find the potential trips that depart the latest
    while still meeting the uncertainty requirement
    src is the current location
    dst is the destination
    subgraph is the set of edges valid under the constraints of the query
    visited is the set of already visited stations in the current path
    curr_path is the current path
    curr_prob is the probability that the current path is possible
    curr_time is the time we arrived at the current location
    curr_lateAvg is the expected delay on current_time
    curr_lateStd is the standard deviation of the delay
    min_trip_departure_time is the time when we can start moving from the first station
    max_trip_arrival_time is the time when we must arrive at the destination at the latest
    paths is the list of possible paths meeting the requirements that we build in this function
    last_line_taken is the line we last took to arrive at the current location
    time_limits is a dictionary mapping stations to latest possible departure time to arrive at the destination in time
    min_prob_success is the uncertainty requirement we have to respect
    best_times is a dictionary containing a single entry where we store the latest departure time for which we can find a path
    that we currently know
    """

    visited.add(src)

    if src == dst:
        final_prob = compute_prob(curr_time, curr_lateAvg, curr_lateStd, max_trip_arrival_time) * curr_prob
        if final_prob >= min_prob_success:
            final_path = curr_path.copy()
            final_path.append(curr_time)
            final_path.append(final_prob)

            dep = compute_dep_time(min_trip_departure_time, final_path[:-2], None)
            if dep > best_times['dep']:
                best_times['dep'] = dep

            paths.append(final_path)

    else:
        vertice_edges = subgraph.out_edges(src, data=True)
        for vertice_edge in vertice_edges:
            edge = vertice_edge[2]

            if edge['dst'] not in visited and edge['line'] != last_line_taken:

                if edge['type'] == 'walk':
                    new_time = add_time(curr_time, edge['lateAvg'])

                    if new_time <= max_trip_arrival_time and \
                       edge['dst'] in time_limits and new_time <= time_limits[edge['dst']]:

                        curr_path.append(edge)
                        compute_paths_arrival_mode(edge['dst'], dst, subgraph, visited, curr_path,
                                                   curr_prob, new_time, curr_lateAvg, curr_lateStd,
                                                   min_trip_departure_time, max_trip_arrival_time, paths,
                                                   edge['line'], time_limits, min_prob_success, best_times)
                        curr_path.pop();

                elif edge['departure_time'] > curr_time and edge['dst'] in time_limits and \
                     edge['arrival_time'] <= time_limits[edge['dst']]:

                    dep = compute_dep_time(curr_time, curr_path, edge = edge)

                    prob = compute_prob(curr_time, curr_lateAvg, curr_lateStd, edge['departure_time'])
                    new_prob = curr_prob * prob

                    if dep >= best_times['dep'] and new_prob >= min_prob_success:
                        curr_path.append(edge)
                        compute_paths_arrival_mode(edge['dst'], dst, subgraph, visited, curr_path,
                                                   new_prob, edge['arrival_time'], edge['lateAvg'], edge['lateStd'],
                                                   min_trip_departure_time, max_trip_arrival_time, paths,
                                                   edge['line'], time_limits, min_prob_success, best_times)
                        curr_path.pop();

    visited.remove(src)


def compute_paths_departure_mode(src, dst, subgraph, visited, curr_path,
                                 curr_prob, curr_time,
                                 min_trip_departure_time, max_trip_arrival_time,
                                 paths, last_line_taken, time_limits, min_prob_success, best_times):
    """
    Use the depth first search algorithm on the subgraph to find the potential trips that arrive the earliest
    while still meeting the uncertainty requirement. In this function we begin our search from the destination to the source.
    src is the source (departure station)
    dst is the current location
    subgraph is the set of edges valid under the constraints of the query
    visited is the set of already visited stations in the current path
    curr_path is the current path
    curr_prob is the probability that the current path is possible
    curr_time is the time we depart from the current location
    min_trip_departure_time is the time when we can start moving from the first station
    max_trip_arrival_time is the time when we must arrive at the destination at the latest
    paths is the list of possible paths meeting the requirements that we build in this function
    last_line_taken is the line we last take to arrive at the next location from the current location
    time_limits is a dictionary mapping stations to earliest possible arrival time in order to depart from the source
    after the min departure time
    min_prob_success is the uncertainty requirement we have to respect
    best_times is a dictionary containing a single entry where we store the earliest arrival time for which we can find a path
    that we currently know
    """

    visited.add(dst)

    if src == dst:
        final_path = curr_path.copy()
        final_path.append(curr_time)
        final_path.append(curr_prob)

        arr = compute_arr_time(max_trip_arrival_time, final_path[:-2], None)
        if arr < best_times['arr']:
            best_times['arr'] = arr

        paths.append(final_path)

    else:
        vertice_edges = subgraph.in_edges(dst, data=True)
        for vertice_edge in vertice_edges:
            edge = vertice_edge[2]

            if edge['src'] not in visited and edge['line'] != last_line_taken:

                if edge['type'] == 'walk':
                    new_time = sub_time(curr_time, edge['lateAvg'])

                    if new_time >= min_trip_departure_time and \
                       edge['src'] in time_limits and new_time >= time_limits[edge['src']]:

                        curr_path.append(edge)
                        compute_paths_departure_mode(src, edge['src'], subgraph, visited, curr_path,
                                                     curr_prob, new_time,
                                                     min_trip_departure_time, max_trip_arrival_time, paths,
                                                     edge['line'], time_limits, min_prob_success, best_times)
                        curr_path.pop();

                elif edge['arrival_time'] < curr_time and edge['src'] in time_limits and \
                     edge['departure_time'] >= time_limits[edge['src']]:

                    arr = compute_arr_time(curr_time, curr_path, edge = edge)

                    prob = compute_prob(edge['arrival_time'], edge['lateAvg'], edge['lateStd'], curr_time)
                    new_prob = curr_prob * prob

                    if arr <= best_times['arr'] and new_prob >= min_prob_success:
                        curr_path.append(edge)
                        compute_paths_departure_mode(src, edge['src'], subgraph, visited, curr_path,
                                                     new_prob, edge['departure_time'],
                                                     min_trip_departure_time, max_trip_arrival_time, paths,
                                                     edge['line'], time_limits, min_prob_success, best_times)
                        curr_path.pop();

    visited.remove(dst)


def dfs(subgraph, departure_station, arrival_station,
        min_trip_departure_time, max_trip_arrival_time, duration,
        min_probability_of_success, mode):
    """
    Use the depth first search algorithm to find the trip that departs / arrives the latest / earliest while
    still meeting the uncertainty requirement of the query
    subgraph is the set of edges valid under the query constraints
    departure_station and arrival station are the source and destination
    min_trip_departure_time is the time when we can start moving
    max_trip_arrival_time is the time when we must have arrived at the latest
    duration is the difference of the two times
    min_probability_of_success is the uncertainty requirement we have to respect
    mode is either 'departure' (arrive at the destination at the earliest possible moment)
    or 'arrival' (depart from the source at the latest possible moment)
    """

    reverse = mode == 'departure'

    # Find the latest departures / earliest arrivals for each reachable station
    time_limits = find_shortest_path(subgraph, departure_station, arrival_station,
                                     min_trip_departure_time, max_trip_arrival_time, duration,
                                     get_all_destinations=True, reverse=reverse)

    # Find the departure / arrival time of the fastest 100% successful trip
    fastest_certain_path = find_shortest_path(subgraph, departure_station, arrival_station,
                                              min_trip_departure_time, max_trip_arrival_time, duration,
                                              get_all_destinations=False, certain_path=True, reverse=reverse)

    visited = set()
    curr_time = min_trip_departure_time if mode =='arrival' else max_trip_arrival_time
    curr_path = [departure_station] if mode == 'arrival' else [arrival_station]
    paths = []

    if fastest_certain_path != "no solution":
        best_times = {'dep': fastest_certain_path} if mode == 'arrival' else {'arr': fastest_certain_path}
    else:
        best_times = {'dep': min_trip_departure_time} if mode == 'arrival' else {'arr': max_trip_arrival_time}

    # Compute the paths
    if mode == 'arrival':
        compute_paths_arrival_mode(departure_station, arrival_station, subgraph,
                                   visited, curr_path, 1.0, curr_time, 0.0, 0.0, min_trip_departure_time,
                                   max_trip_arrival_time, paths, '', time_limits,
                                   min_probability_of_success, best_times)
    else:
        compute_paths_departure_mode(departure_station, arrival_station, subgraph,
                                     visited, curr_path, 1.0, curr_time, min_trip_departure_time,
                                     max_trip_arrival_time, paths, '', time_limits,
                                     min_probability_of_success, best_times)

    if not paths:
        return {'departure time' : '', 'arrival_time' : '',
                'duration' : '', 'probability' : 0.0, 'path': []}

    # Compute the departure / arrival time and duration of the possible trips
    if mode == 'arrival':
        dep_times = [compute_dep_time(max_trip_arrival_time, path[:-2], None) for path in paths]
        best_indices = np.where(np.array(dep_times) == max(dep_times))[0]
    else:
        arr_times = [compute_arr_time(min_trip_departure_time, path[:-2], None) for path in paths]
        best_indices = np.where(np.array(arr_times) == min(arr_times))[0]

    best_paths = []
    durations = []
    probabilities = []

    for idx in best_indices:
        best_path = paths[idx]

        path_edges = best_path[1:-2]
        if mode == 'departure':
            path_edges.reverse()

        corrected_edges = []
        # Compute the departure and arrival time for walk edges and remove unnecessary data
        for idx, e in enumerate(path_edges):
            edge = e.copy()

            if edge['type'] == 'walk':
                if idx == 0:
                    if len(path_edges) == 1:
                        if mode == 'arrival':
                            edge['arrival_time'] = max_trip_arrival_time
                            edge['departure_time'] = sub_time(edge['arrival_time'], edge['lateAvg'])
                        else:
                            edge['departure_time'] = min_trip_departure_time
                            edge['arrival_time'] = add_time(edge['departure_time'], edge['lateAvg'])
                    else:
                        edge['arrival_time'] = path_edges[1]['departure_time']
                        edge['departure_time'] = sub_time(edge['arrival_time'], edge['lateAvg'])
                else:
                    edge['departure_time'] = path_edges[idx - 1]['arrival_time']
                    edge['arrival_time'] = add_time(edge['departure_time'], edge['lateAvg'])

                edge.pop('line');

            edge.pop('departure_day');
            edge.pop('lateAvg');
            edge.pop('lateStd');
            corrected_edges.append(edge)


        duration_time = compute_time_between(corrected_edges[0]['departure_time'], corrected_edges[-1]['arrival_time'])
        probability = best_path[-1]

        best_paths.append(corrected_edges)
        durations.append(duration_time)
        probabilities.append(probability)

    # Pick the trips with the lowest duration among the remaining best trips
    best_duration_indices = np.where(np.array(durations) == min(durations))[0]

    best_duration_paths = [best_paths[idx] for idx in best_duration_indices]
    best_duration_probs = [probabilities[idx] for idx in best_duration_indices]
    best_duration = durations[best_duration_indices[0]]

    # Pick the trip with the highest success probability among the remaining best trips
    best_probability_index = np.argmax(best_duration_probs)

    path_edges = best_duration_paths[best_probability_index]
    best_probability = best_duration_probs[best_probability_index]


    # Remove prefix from the departure / arrival time
    for edge in path_edges:
        edge['departure_time'] = edge['departure_time'][2:]
        edge['arrival_time'] = edge['arrival_time'][2:]

    departure_time = path_edges[0]['departure_time']
    arrival_time = path_edges[-1]['arrival_time']

    return {'departure_time' : departure_time, 'arrival_time' : arrival_time,
            'duration' : best_duration, 'probability': best_probability, 'path': path_edges}


def plan(departure_station, arrival_station=None,
         start_datetime=None, end_datetime=None,
         min_probability_of_success=0.0,
         heatmap=False, heatmap_duration=10):
    """
    Parses the query, creates the subgraph of valid edges in the context of the query, and calls the appropriate
    function to compute the result.
    """

    if min_probability_of_success < 0.0:
        min_probability_of_success = 0.0
    elif min_probability_of_success > 1.0:
        min_probability_of_success = 1.0

    if heatmap:
        if start_datetime is None:
            return {}
        else:
            if heatmap_duration < 0:
                heatmap_duration = 0
            elif heatmap_duration > 120:
                heatmap_duration = 120
            end_datetime = start_datetime + timedelta(minutes=heatmap_duration)

    else:
        if (start_datetime is None and end_datetime is None) or \
           (start_datetime is not None and end_datetime is not None) or \
            arrival_station is None:
            return {'departure time' : '', 'arrival_time' : '',
                    'duration' : '', 'path': []}

        elif start_datetime is None:
            mode = 'arrival'
            start_datetime = end_datetime - timedelta(hours=2)

        else:
            mode = 'departure'
            end_datetime = start_datetime + timedelta(hours=2)

    start_time  = str(start_datetime.time())
    end_time = str(end_datetime.time())

    start_day  = calendar.day_name[start_datetime.weekday()]
    end_day = calendar.day_name[end_datetime.weekday()]

    min_trip_departure_time = '0-' + start_time

    end_time_prefix = '0-' if (start_day == end_day) else '1-'
    max_trip_arrival_time = end_time_prefix + end_time

    duration = (end_datetime - start_datetime).seconds

    # Filter out the edges that can't be used for the query
    filtered_edges = get_filtered_edges(start_day, end_day, start_time, end_time, duration).toPandas()

    def to_dt(time):
        """
        Convert the time to a useful format for the implementation of the algorithm
        """

        if time == 'null':
            return 'null'
        elif time >= start_time:
            return '0-' + time
        else:
            return '1-' + time


    filtered_edges['departure_time'] = filtered_edges['departure_time'].map(lambda x: to_dt(x))
    filtered_edges['arrival_time']   = filtered_edges['arrival_time'].map(lambda x: to_dt(x))

    # Create a networkx graph of all the valid edges
    subgraph = nx.from_pandas_edgelist(filtered_edges, 'src', 'dst', edge_attr=True, create_using=nx.MultiDiGraph())

    if heatmap:
        return compute_heatmap(subgraph, departure_station, departure_station,
                               min_trip_departure_time, max_trip_arrival_time, duration,
                               min_probability_of_success)

    else:
        return dfs(subgraph, departure_station, arrival_station, min_trip_departure_time,
                   max_trip_arrival_time, duration, min_probability_of_success, mode)

def create_app():
    app = Flask(__name__)

    app.register_blueprint(routes.bp)
    app.config.update(
        DEBUG=True,
        TEMPLATES_AUTO_RELOAD=True
    )

    return app
