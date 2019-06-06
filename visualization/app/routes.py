from flask import Blueprint, render_template, request, redirect, url_for
from source.constants import station_list
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

        find_path_and_save_map(departure_station, arrival_station, startDateTime, endDateTime, min_probability_of_sucess)

        return redirect(url_for('index.plot_trip'))

@bp.route('/plot_trip', methods=('GET', 'POST'))
def plot_trip():
    if request.method == 'GET':
        return render_template('plot_trip.html', stations = station_list)
    elif request.method == 'POST':
        departure_station = request.form['departure_station']
        arrival_station = request.form['arrival_station']
        startDateTime = request.form['startDateTime']
        endDateTime = request.form['endDateTime']
        min_probability_of_sucess = request.form['min_probability_of_sucess']

        find_path_and_save_map(departure_station, arrival_station, startDateTime, endDateTime, min_probability_of_sucess)

        return redirect(url_for('index.plot_trip'))
