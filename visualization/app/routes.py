from flask import Blueprint, render_template, request, redirect, url_for
from source.constants import station_list
import datetime
from datetime import datetime
from source.find_path import find_path_and_save_map


bp = Blueprint('index', __name__)

@bp.route('/', methods=('GET', 'POST'))
def homepage():
    if request.method == 'GET':
        return render_template('homepage.html', stations = station_list)
    elif request.method == 'POST':
        departure_station = request.form['departure_station']
        arrival_station = request.form['arrival_station']
        startDateTime = request.form['startDateTime']
        endDateTime = request.form['endDateTime']
        min_probability_of_sucess = request.form['min_probability_of_sucess']
        heatmap_duration = request.form['heatmap_duration']

        if heatmap_duration == '':
            heatmap_duration = 0

        print('aaaaaaaaaa')
        print(min_probability_of_sucess)
        print(type(min_probability_of_sucess))

        if min_probability_of_sucess == '':
            min_probability_of_sucess = 0.95
        else:
            min_probability_of_sucess = float(min_probability_of_sucess)

        print('bbbbbbbbbbb')
        print(min_probability_of_sucess)
        print(type(min_probability_of_sucess))

        if endDateTime != '':
            endDateTime = datetime.strptime(endDateTime, '%Y-%m-%dT%H:%M')
        elif startDateTime != '':
            startDateTime = datetime.strptime(startDateTime, '%Y-%m-%dT%H:%M')

        trip_result = find_path_and_save_map(departure_station, arrival_station,
         startDateTime=startDateTime, endDateTime=endDateTime,
         min_probability_of_sucess=min_probability_of_sucess,
         heatmap_duration=heatmap_duration)

        return redirect(url_for('index.plot_trip', trip_result = trip_result))

@bp.route('/plot_trip', methods=('GET', 'POST'))
def plot_trip():
    if request.method == 'GET':
        return render_template('plot_trip.html', stations = station_list, trip_result = request.args['trip_result'])
    elif request.method == 'POST':
        departure_station = request.form['departure_station']
        arrival_station = request.form['arrival_station']
        startDateTime = request.form['startDateTime']
        endDateTime = request.form['endDateTime']
        min_probability_of_sucess = request.form['min_probability_of_sucess']

        find_path_and_save_map(departure_station, arrival_station, startDateTime, endDateTime, min_probability_of_sucess)

        return redirect(url_for('index.plot_trip'))
