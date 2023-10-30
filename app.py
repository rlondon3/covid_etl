from transformations.extract_data import extract
from handlers.covid import covid_routes
from database import ( connect )
from flask import Flask
from models.covid import Covid_Store

# Get schema methods
store = Covid_Store()

# Get data from JHU and NYT
extract()

app = Flask(__name__)

@app.get("/")
def connect_db():
    connected = connect()
    print(connected)
    if connected:
        # Create the covid table in database
        store.create()
        # Upload covid data to database
        store.update()

covid_routes = covid_routes(app)