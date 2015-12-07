# --
# time-example.py
# scrape all of the posts in a (small) geographic area for a given time

import json
from binsta import *

config = json.load(open('config.json'))

from elasticsearch import Elasticsearch
client = Elasticsearch([{'host' : 'localhost', 'port' : 9205}])

start_time = datetime.strptime('2015-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
end_time   = datetime.strptime('2015-01-01 00:10:00', '%Y-%m-%d %H:%M:%S')

params = {
    'lat'           : 40.7078,
    'lng'           : -74.0119,
    'distance'      : 1000, 
    'min_timestamp' : time_to_int(start_time),
    'max_timestamp' : time_to_int(end_time),
    'client_id'     : config['client_id'],
    'count'         : 500
}

def es_logger(data, params):
    return log_to_elasticsearch(data, params, client)

data = scrape_over_time(params, callbacks = [log_to_disk, es_logger])
