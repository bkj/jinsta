# --
# time-example.py
# scrape all of the posts in a (small) geographic area for a given time

import json
from binsta import *

config = json.load(open('config.json'))

from elasticsearch import Elasticsearch
client = Elasticsearch([{'host' : 'localhost', 'port' : 9205}])

# def es_logger(data, params):
#     return log_to_elasticsearch(data, params, client)

# --

start_time = datetime.strptime('2015-12-07 00:00:00', '%Y-%m-%d %H:%M:%S')
end_time   = datetime.strptime('2015-12-07 13:00:00', '%Y-%m-%d %H:%M:%S')

params = {
    'lat'           : 40.7078,
    'lng'           : -74.0119,
    'distance'      : 5000, 
    'min_timestamp' : time_to_int(start_time),
    'max_timestamp' : time_to_int(end_time),
    'client_id'     : config['client_id'],
    'count'         : 500
}

# Fixed time
data = scrape_over_time(params, callbacks = [log_to_disk], prospective = True)

# Prospective scrape
# data = scrape_over_time(params, callbacks = [log_to_disk], prospective = True)


times    = np.array(map(lambda x: float(x['created_time']), data))
reqtimes = np.array(map(lambda x: (
    x['req_params']['min_timestamp'],
    x['req_params']['max_timestamp']
), data))

reqtimes[:,0] - times
reqtimes[:,1] - times

plt.plot(reqtimes[:,0])
plt.plot(reqtimes[:,1])
plt.show()