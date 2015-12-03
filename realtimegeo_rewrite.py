import json
import copy
import urllib2, urllib
import numpy as np
from datetime import datetime
from time import mktime, sleep
from geopy.distance import great_circle

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


# Try to fetch a specific parameterization
def fetch_block(params):
    try:
        reqstr   = make_reqstr(params)
        print reqstr
        response = urllib2.urlopen(reqstr)
        return json.loads(response.read())['data'], False
    except:
        print 'error fetching block :: %s' % str(params)
        return None, True


# Split an interval in half
def split_interval(low, high):
    mid = float(low + high) / 2
    return (low, mid), (mid, high), high - mid


# Scrape across time
# TODO : Add callback to save this incrementally
def scrape_over_time(params, max_images = 20, min_time = -1):
    sleep(1)
    print bcolors.OKBLUE + 'scraping :: %s' % str(params) + bcolors.ENDC
    
    data, err = fetch_block(params)
    
    if not err:
        print bcolors.OKGREEN + 'number of hits :: %d' % len(data) + bcolors.ENDC
        
        if len(data) > max_images:
            
            int1, int2, diff = split_interval(params['min_timestamp'], params['max_timestamp'])
            
            # TODO : Handle case where diff is too small
            if diff < min_time:
                return data
            
            print bcolors.WARNING + '\t -- splitting -- ' + bcolors.ENDC
            # First time subset
            p1 = copy.copy(params)
            p1['min_timestamp'] = int1[0]
            p1['max_timestamp'] = int1[1]
            
            # Second time subset
            p2 = copy.copy(params)
            p2['min_timestamp'] = int2[0]
            p2['max_timestamp'] = int2[1]
            
            data1 = scrape_over_time(p1)
            data2 = scrape_over_time(p2)
            
            # TODO : Maybe also return data, since API is sortof nondeterministic
            return data1 + data2
        else:
            return data
        
    else:
        print bcolors.FAIL + 'error at scrape time :: %s' % str(params) + bcolors.ENDC
        # Do we want to move on to the next time slice completely?
        return []


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
# Examples

# --
# Ex 1
# Scrape a given area over time
start_time = datetime.strptime('2015-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
end_time   = datetime.strptime('2015-01-01 00:10:00', '%Y-%m-%d %H:%M:%S')

params = {
    'lat'           : 40.7078,
    'lng'           : -74.0119,
    'distance'      : 500, 
    'min_timestamp' : time_to_int(start_time),
    'max_timestamp' : time_to_int(end_time),
    'client_id'     : 'a2a5406ce7d548489e3e23ce5bee7ffe',
    'count'         : 500
}

data = scrape_over_time(params)

# --
# Ex 2
# Tile an area with circles, then scrape each circle over time

start_time = datetime.strptime('2015-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
end_time   = datetime.strptime('2015-01-01 00:10:00', '%Y-%m-%d %H:%M:%S')

min_lat = -74.02133
max_lat = -73.96863
min_lon = 40.700748
max_lon = 40.746412

params = {
    'distance'      : 500, 
    'min_timestamp' : time_to_int(start_time),
    'max_timestamp' : time_to_int(end_time),
    'client_id'     : 'a2a5406ce7d548489e3e23ce5bee7ffe',
    'count'         : 500
}

center  = (max_lat - min_lat) / 2, (max_lon - min_lon) / 2
r       = meters2latlon(params['distance'], center[0], center[1])[0]
centers = circle_cover(min_lat, max_lat, min_lon, max_lon, r)

data = {}
for c in centers:
    print '\n [lat, lng]:: ' + str(c)
    params['lat'] = c[0]
    params['lng'] = c[1]
    data[str(c)] = scrape_over_time(params)



# --
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
# ---


min_lat = 0
max_lat = 10
min_lon = 0
max_lon = 10

inc = 
circle_cover(min_lat, max_lat, min_lon, max_lon)

