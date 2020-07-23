import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc, inspect

from flask import Flask, jsonify
from datetime import datetime as dt, datetime
from datetime import timedelta

#create engine from the same sqlite file
engine = create_engine("sqlite:///Resources/hawaii.sqlite",
    connect_args={'check_same_thread': False})


# reflect an existing database into a new model
Base = automap_base()


Base.prepare(engine, reflect=True)

Base.metadata.create_all(engine)
Base.classes.keys()

# reflect the tables
measurement= Base.classes.measurement
station= Base.classes.station

session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#Home page.List all routes that are available.
@app.route("/")
def homepage():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    """Convert the query results to a dictionary using date as the key and prcp as the value."""
    # Query all passengers
    results = session.query(measurement.date,measurement.prcp).\
        order_by(measurement.date).all()

    #close session
    session.close()

    # name a list for date and prcp
    datalist=[]

    # loop date as column and prcp as values
    for date, prcp in results:
        #create a dict to call and append the list with the dict
        newlist= {}
        newlist[date]= prcp
        datalist.append(newlist)

    #Return the JSON representation of your dictionary.
    return jsonify(datalist)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    # Return a JSON list of stations from the dataset.
    results= session.query(station.station).all()

    #close session
    session.close()

    #list the names
    stationlist= list(np.ravel(results))

    #Return the JSON representation of your dictionary.
    return jsonify(stationlist)


@app.route("/api/v1.0/tobs")
def tobs():

    #Query the dates and temperature observations of the most active station for the last year of data.
    results = session.query(measurement.station,measurement.date,measurement.prcp,measurement.tobs).\
        order_by(measurement.prcp.desc()).all()
    # set dataframe to filter useful info 
    df= pd.DataFrame(results, columns = ['station','date','prcp', 'tobs'])
    date= df['date'].max()

    # to covert string to date - dt.strptime(str, '%Y-%m-%d')
    maxDate = dt.strptime(date,'%Y-%m-%d')
    prevYear = maxDate - timedelta(days = 365)
    
    # filter the most active station 
    most= session.query(measurement.station, func.count(measurement.station).label('counts')).\
            group_by(measurement.station).\
            order_by(desc("counts")).limit(1)[0][0]

    # query conditions 
    highest= session.query(measurement.date, measurement.tobs).\
        filter(measurement.station == most).\
        filter(measurement.date >= prevYear).\
        order_by(measurement.date.desc()).all()
    
    #close session
    session.close()

    # create a list for date and tobs
    templist=[]

    # loop in the query condition
    for date, tobs in highest:

        #create a dict to call the values 
        newlist= {}

        # dict column date is for date value, column tobs is for tobs value
        newlist["date"]= date
        newlist["tobs"]= tobs

        # append the list with dict loop
        templist.append(newlist)

    #Return the JSON representation of your dictionary.
    return jsonify(templist)



@app.route("/api/v1.0/<start>")
# route example: /2015-07-14

def startdate(start):
    
    #Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    start = datetime.strptime(start, "%Y-%m-%d").date()
    #maxDate = dt.strptime(maxDate,'%Y-%m-%d').date()

    #Query temp's min,avg and max in general
    sel = [func.min(measurement.tobs).label('tmin'), func.avg(measurement.tobs).label('tavg'), func.max(measurement.tobs).label('tmax')]
    
    #Query all temps from start date
    results= session.query(*sel).filter(measurement.date >= start).all()
    
    #close session
    session.close()

    #create a list to loop min, avg and max
    startlist = []
    for tmin, tavg, tmax in results:
        newlist= {}
        newlist['tmin'] = tmin
        newlist['tavg'] = tavg
        newlist['tmax'] = tmax
        startlist.append(newlist)
    
    #Return the JSON representation of your dictionary.
    return jsonify(startlist)

@app.route("/api/v1.0/<start>/<end>")
# route example: /api/v1.0/2015-07-14/2015-07-24
def enddate(start,end):

    start = datetime.strptime(start, "%Y-%m-%d").date()
    end= datetime.strptime(end, "%Y-%m-%d").date()

    sel = [func.min(measurement.tobs).label('tmin'), func.avg(measurement.tobs).label('tavg'), func.max(measurement.tobs).label('tmax')]
    
    #When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.
    results= session.query(*sel).\
            filter(measurement.date >= start).\
            filter(measurement.date <= end).all()
    
    #close session
    session.close()

    startlist = []
    for tmin, tavg, tmax in results:
        newlist= {}
        newlist['tmin'] = tmin
        newlist['tavg'] = tavg
        newlist['tmax'] = tmax
        startlist.append(newlist)
    
    #Return the JSON representation of your dictionary.
    return jsonify(startlist)

if __name__ == '__main__':
    app.run(debug=True)
