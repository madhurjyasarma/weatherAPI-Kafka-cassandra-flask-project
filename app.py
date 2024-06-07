from flask import Flask, request, render_template
from cassandra.cluster import Cluster

app = Flask(__name__)

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_keyspace')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    city = request.form['city']
    query = "SELECT * FROM weather_table WHERE city=%s ALLOW FILTERING"
    rows = session.execute(query, [city])
    rows_list = list(rows)  # Convert ResultSet to list
    return render_template('results.html', rows=rows_list, city=city)

if __name__ == '__main__':
    app.run(debug=True)
