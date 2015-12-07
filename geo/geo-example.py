# --
# geo-example.py
# Scrape all of the posts in a (large) geographic area for a given time

from binsta import *
import json

config = json.load(open('config.json'))

start_time = datetime.strptime('2015-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
end_time   = datetime.strptime('2015-01-01 00:10:00', '%Y-%m-%d %H:%M:%S')

params = {
    'lat' : { 'min' : -74.02133, 'max' : -73.96863 },
    'lon' : { 'min' : 40.700748, 'max' : 40.746412 },
    'distance'      : 500, 
    'min_timestamp' : time_to_int(start_time),
    'max_timestamp' : time_to_int(end_time),
    'client_id'     : config['client_id'],
    'count'         : 500
}

data = scrape_over_geo(params)


time_to_int(datetime.now())