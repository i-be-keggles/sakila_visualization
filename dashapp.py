import dash
from dash import html,dcc
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine
import time
import math
from datetime import datetime

pss = ""

# Connect to the Sakila database
engine = create_engine(f'mysql://root:{pss}@localhost/sakila')

import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = "LtpiuFn0wJKrIRxJSizwvMRc4SYJ8eyz1lIqptW8tttr7QOfnSIs7ADV8AlYqv70FK3iaMhR_d6NXu1DtrQCOw=="
org = "Luisiana"
url = "http://localhost:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

bucket="GrassBucket"

write_api = write_client.write_api(write_options=SYNCHRONOUS)
delete_api = write_client.delete_api()
delete_api.delete("1700-01-01T00:00:00Z", "2250-01-01T00:00:00Z", "", bucket)

query = """
SELECT 
    DATE(r.rental_date) AS rental_day,
    cat.name AS category_name,
    COUNT(*) AS rental_count
FROM
    rental AS r
        JOIN
    inventory AS i ON r.inventory_id = i.inventory_id
        JOIN
    film AS f ON i.film_id = f.film_id
        JOIN
    film_category AS fc ON f.film_id = fc.film_id
        JOIN
    category AS cat ON fc.category_id = cat.category_id
GROUP BY rental_day , category_name
ORDER BY rental_day , category_name;
"""

rental_data = pd.read_sql(query, engine)

rental_data["rental_day"] = pd.to_datetime(rental_data["rental_day"])
rental_data["rental_day"] = rental_data["rental_day"].apply(lambda x: int(x.timestamp()))

for i, row in rental_data.iterrows():
    point = (
    Point("rental_count")
    .tag("category", row["category_name"])
    .field("rental_count", row["rental_count"])
    .time(row["rental_day"])
    )
    write_api.write(bucket=bucket, org="Luisiana", record=point)
    print(row["rental_day"])









# Create the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    html.H1("Sakila Rental Data Over Time"),
    
    # Dropdown to select a category
    dcc.Dropdown(
        id='category-dropdown',
        options=[
            {'label': 'Category 1', 'value': 1},
            {'label': 'Category 2', 'value': 2},
            # Add more options based on your data
        ],
        value=1  # Default selected option
    ),
    
    # Line chart to display data over time
    dcc.Graph(id='line-chart')
])

# Define callback to update the line chart based on the selected category
@app.callback(
    Output('line-chart', 'figure'),
    [Input('category-dropdown', 'value')]
)
def update_line_chart(selected_category):
    # SQL query to retrieve data for the selected category over time
    query = """
    SELECT DATE(rental_date) AS rental_day, COUNT(rental_id) AS rental_count
    FROM rental, inventory, film, film_category
    WHERE rental.inventory_id = inventory.inventory_id AND
    inventory.film_id = film.film_id AND
    film.film_id = film_category.film_id AND
    category_id = 1
    GROUP BY rental_day;
    """

    rental_data = pd.read_sql(query, engine)

    # Create the line chart
    fig = {
        'data': [
            {
                'x': rental_data['rental_day'],
                'y': rental_data['rental_count'],
                'type': 'bar',
                'marker': {'color': 'blue'}
            }
        ],
        'layout': {
            'title': f'Rental Count for Category {selected_category}',
            'xaxis': {'title': 'Rental Day'},
            'yaxis': {'title': 'Rental Count'}
        }
    }

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)


#update_line_chart()

#engine.dispose()
#write_client.dispose()