# --
# geo-example.py
# Scrape all of the posts in a (large) geographic area for a given time

from binsta import *

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

