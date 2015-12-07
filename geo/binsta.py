import json
import copy
import itertools
import urllib2, urllib
import numpy as np
from hashlib import md5
from datetime import datetime
from time import mktime, sleep
from geopy.distance import great_circle

# If you want to log to elasticsearch:
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Printing in color
class bcolors:
    HEADER    = '\033[95m'
    OKBLUE    = '\033[94m'
    OKGREEN   = '\033[92m'
    WARNING   = '\033[93m'
    FAIL      = '\033[91m'
    ENDC      = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'


# Format string for instagram
def make_reqstr(params, qtype = 'geo'):
    if qtype == 'geo':
        return 'https://api.instagram.com/v1/media/search?' + urllib.urlencode(params)
    else:
        return None


# Try to fetch a specific geotemporal block of data
def fetch_block(params):
    try:
        qid      = md5(json.dumps(params)).hexdigest()
        
        reqstr   = make_reqstr(params)
        response = urllib2.urlopen(reqstr)
        data     = json.loads(response.read())['data']
        _ = map(lambda x: x.update({'binsta' : {'params' : params, 'qid' : qid}}), data)
        return data, False
    except:
        print 'error fetching block :: %s' % str(params)
        return None, True

# --
# Functions for scraping over time

# NB : Prospective scrapes will/should never terminate, so you need to use
# a callback

def pprint_params(params):
    return '%s | %s | (%f, %f)' % (
        datetime.utcfromtimestamp(params['min_timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
        datetime.utcfromtimestamp(params['max_timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
        params['lat'],
        params['lng']
    )

# Split an interval in half
def split_interval(low, high):
    mid = float(low + high) / 2
    return (low, mid), (mid, high), high - mid

# This should sleep (for a couple of minutes) if there's too much overlap.
# Don't need to waste requests scraping the same period over and over again
#
# What should time_margin be?
def step_forward(params, time_margin = 60 * 4):
    params = copy.copy(params)
    
    # Take a timestep forward
    old_maxtimestamp = params['max_timestamp']
    now              = time_to_int(datetime.now())
    
    params['max_timestamp'] = now
    params['min_timestamp'] = min(old_maxtimestamp, now - time_margin)
    
    return params


# Scrape across time

# TODO : Should never request time window larger than N minutes (N = 30, maybe?)
MIN_TIME   = 60 # Don't make requests smaller than a minute
MAX_IMAGES = 99 # If we get more than 99 hits, need to split

def scrape_over_time(params, prospective = False, callbacks = False, depth = 0):
    
    if prospective and (depth == 0):
        print bcolors.WARNING + '! convert max_timestamp -> current time' + bcolors.ENDC
        params['max_timestamp'] = time_to_int(datetime.now())
    
    prefix = ''.join(['\t' for _ in range(depth)])
    sleep(1)
    print bcolors.OKBLUE + prefix + pprint_params(params) + bcolors.ENDC
    
    data, err = fetch_block(params)
    
    if not err:
        print bcolors.OKGREEN + prefix + '$ %d hits' % len(data) + bcolors.ENDC
        
        if len(data) > MAX_IMAGES:
            
            int1, int2, diff = split_interval(params['min_timestamp'], params['max_timestamp'])
            
            if diff < MIN_TIME:
                print bcolors.WARNING + prefix + '! below min time increment' + bcolors.ENDC
                out = data
                
            else:
                print bcolors.WARNING + prefix + '! splitting' + bcolors.ENDC
                
                # First time subset
                p1 = copy.copy(params)
                p1['min_timestamp'] = int1[0]
                p1['max_timestamp'] = int1[1]
                
                # Second time subset
                p2 = copy.copy(params)
                p2['min_timestamp'] = int2[0]
                p2['max_timestamp'] = int2[1]
                
                data1 = scrape_over_time(p1, callbacks = callbacks, depth = depth + 1)
                data2 = scrape_over_time(p2, callbacks = callbacks, depth = depth + 1)
                
                # TODO : Maybe also return data, since API is (sortof) nondeterministic
                out = data1 + data2
                if len(data) != len(out):
                    print bcolors.FAIL + prefix + '* %d < %d (size violation)' % (len(data), len(out))  + bcolors.ENDC
        else:
            # Add request parameters to the objects that get returned
            _ = map(lambda x: x.update({'req_params' : params}), data)
            
            # do callback iff we get a satisfactory time slice
            if callbacks:
                [callback(data, params) for callback in callbacks]
            
            print bcolors.OKGREEN + prefix + '+ return' + bcolors.ENDC
            out = data
        
    else:
        print bcolors.FAIL + 'error at scrape time :: %s' % str(params) + bcolors.ENDC
        # Do we want to move on to the next time slice completely?
        out = []
    
    if prospective and (depth == 0):
        print bcolors.FAIL + '-> stepping forward' + bcolors.ENDC
        _ = scrape_over_time(step_forward(params), prospective = True, callbacks = callbacks, depth = 0)
    else:
        return out


def scrape_over_geo(params, prospective = False, callbacks = False):
    
    center = (
        (params['lat']['max'] - params['lat']['min']) / 2,
        (params['lon']['max'] - params['lon']['max']) / 2
    )
    
    r = meters2latlon(params['distance'], center[0], center[1])[0]
    
    centers = circle_cover(
        params['lat']['min'],
        params['lat']['max'],
        params['lon']['min'],
        params['lon']['max'], 
        r
    )
    
    data = {}
    for c in centers:
        print '\n [lat, lng]:: ' + str(c)
        params['lat'] = c[0]
        params['lng'] = c[1]
        data[str(c)]  = scrape_over_time(params, callbacks = callbacks, prospective = False)
    
    if prospective:
        _ = scrape_over_geo(step_forward(params), callbacks = callbacks, prospective = True)
    else:
        return data


# Conver a time object to an integer
def time_to_int(t):
    return int(mktime(t.timetuple()))

# --
# Functions for tiling space with circles

# Cover a rectangle completely with circles
def circle_cover(x0, x1, y0, y1, r):
    width   = x1 - x0
    height  = y1 - y0
    spacing = 1 / np.sqrt(2) * r # Maximum spacing that guarantees full cover
    
    grid = []
    
    current_x = x0 + spacing
    while (current_x <= x1):
        current_y = y0 + spacing
        
        while (current_y <= y1):
            grid.append((current_y, current_x))
            current_y += 2 * spacing
        
        current_x += 2 * spacing
    
    return np.array(grid)

# This is not optimally efficient, but we can replace it with the
# correct circle-packing algorithm later.  Not optimally efficient
# because we're actually tiling the surface of a sphere rather
# than a flat rectangle
def meters2latlon(target, lat, lon, offset = .1, step = 0.001, tolerance = 1):
    center = np.array([lat, lon])
    
    direction     = None
    prevdirection = None
    while True:
        # Distance in X and Y direction can be different
        # Using the minimum of the 2 guarantees that we cover each point
        # at least once
        distlat = great_circle( center, center + np.array([offset, 0]) ).meters
        distlon = great_circle( center, center + np.array([0, offset]) ).meters
        dist    = min(distlat, distlon)
        
        if direction != prevdirection:
            step = step / 2
            
        prevdirection = direction
        if dist < (target - tolerance):
            offset += step
            direction = 1
        elif dist > (target + tolerance):
            offset -= step
            direction = -1
        else:
            return offset, dist
            break


# ----
# IO

def log_to_print(data, params):
    print data
    return True


def log_to_disk(data, params):
    file_name = 'data/%f_%f_%d_%d_bulkjson.txt' % (params['lat'], params['lng'], params['min_timestamp'], params['max_timestamp'])
    try:
        json.dump(data, open(file_name, "w"))
        return True
    except:
        return False


def log_to_elasticsearch(data, params, client, index = 'test', doc_type = 'test', chunk_size = 10):
    try:
        data_gen = itertools.imap(lambda d: {"_index" : index, "_type" : doc_type, "_op_type" : "index", "_id" : d['id'], "source" : d}, data)
        for a, b in streaming_bulk(client, data_gen, chunk_size = chunk_size):
            pass
        
        return True
    except:
        return False


# ********************
# -- QA/QC --
if False:
    from matplotlib import pyplot as plt

    d = np.hstack(data.values())

    times = sorted(map(lambda x: int(x['created_time']), d))
    plt.plot(times)
    plt.show()

    datetime.utcfromtimestamp(min(times)).strftime('%Y-%m-%d %H:%M:%S')
    datetime.utcfromtimestamp(max(times)).strftime('%Y-%m-%d %H:%M:%S')


    locs = map(lambda x: x['location'], d)

    from matplotlib import pyplot as plt

    plt.scatter(
        map(lambda x: x['latitude'], locs),
        map(lambda x: x['longitude'], locs)    
    )
    plt.show()
