import json
import copy
import time
import pandas as pd
from binsta import *
from pprint import pprint
from datetime import timedelta
from matplotlib import pyplot as plt

config = json.load(open('config.json'))

from elasticsearch import Elasticsearch
client = Elasticsearch([{'host' : 'localhost', 'port' : 9205}])


# --
# Simple retrospective scraper

# This is exploiting the apprent fact that Instagram returns the
# last 100 posts in the time frame.  I have yet to see an example
# that violates this assumption, though it's possible that this does happen
# (as a function of something like sharding)
def scrape_backward(params, callbacks = False):
    params  = copy.copy(params)
    
    counter = 0
    out     = []
    while True:
        if params['max_timestamp'] < params['min_timestamp']:
            break
        
        print '\t max_timestamp :: \033[92m %s \033[0m' % \
            datetime.fromtimestamp(params['max_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        print '\t min_timestamp :: \033[92m %s \033[0m' % \
            datetime.fromtimestamp(params['min_timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        print '\n'
            
        data, _ = fetch_block(params)
        
        # Add to output
        out += data
        
        # Do callback
        if callbacks:
            [callback(data, params) for callback in callbacks]
        
        if len(data) < 50:
            break
        else:
            params['max_timestamp'] = min([ int(x['created_time']) for x in data])
            counter += 1
    
    return out, counter, params, data


def est_nexttime(times, target = 50, cons = .8):
    diffs       = np.diff(sorted(times))
    offset      = np.round(cons * target * np.mean(diffs[-500:]))
    return datetime.fromtimestamp(max(times)) + timedelta(seconds = offset)


def scrape_forward(params, callbacks = False, init_lookback = 5):
    times  = []
    params = copy.copy(params)
    
    # Get last `init_lookback` minutes
    print '--- getting background rates ---'
    params['max_timestamp'] = time_to_int(datetime.now())
    params['min_timestamp'] = params['max_timestamp'] - (60 * init_lookback)
    
    while True:
        # Get everything in time window
        data  = scrape_backward(params, callbacks = callbacks)[0]
        print 'len data :: %d \n' % len(data)
        
        # Determine time at which to pull next window
        times   += map(lambda x: int(x['created_time']), data)
        nexttime = est_nexttime(times)
        print 'next scrape at \t \033[92m %s \033[0m \n' % nexttime.strftime('%Y-%m-%d %H:%M:%S')
        
        # Step to next time window
        params['min_timestamp'] = params['max_timestamp']
        params['max_timestamp'] = time_to_int(nexttime)
        
        # Wait until time, then continue
        tdiff = nexttime - datetime.now()
        while tdiff.seconds > 0:
            print 'tdiff : %s' % str(tdiff.seconds)
            time.sleep(1)
            tdiff = nexttime - datetime.now()


# --
# Forwards example
params = {
    'lat'       : 40.7078,
    'lng'       : -74.0119,
    'distance'  : 5000, 
    'client_id' : config['client_id'],
    'count'     : 500
}

def es_logger1(data, params):
    return log_to_elasticsearch(data, params, client, index = 'binsta', doc_type = 'testforward')

scrape_forward(params, callbacks = [es_logger1])


# --
# Backwards example


start_time = datetime.strptime('2015-12-06 00:00:00', '%Y-%m-%d %H:%M:%S')
end_time   = datetime.strptime('2015-12-07 00:00:00', '%Y-%m-%d %H:%M:%S')

params = {
    'lat'           : 40.7078,
    'lng'           : -74.0119,
    'distance'      : 5000, 
    'min_timestamp' : time_to_int(start_time),
    'max_timestamp' : time_to_int(end_time),
    'client_id'     : config['client_id'],
    'count'         : 500
}

def es_logger(data, params):
    return log_to_elasticsearch(data, params, client, index = 'binsta', doc_type = 'test')

data, counter, last_params, last_data = scrape_backward(params, callbacks = [es_logger])

data, _ = fetch_block(last_params)
len(data)