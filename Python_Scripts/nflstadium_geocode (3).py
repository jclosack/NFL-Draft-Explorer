"""This script retrieves lat/lon coordinates for NFL stadiums

NFL Stadium names are geocoded using the Google Maps API. Lat/Lon
and stadium addresses are concatenated to the manually compiled
NFL team information. This script writes a "NFL Teams.csv" file
"""
import pandas as pd
import googlemaps
import json

mykeyfile = open("key.txt",'r')
mykey = mykeyfile.readline()
mykeyfile.close()

gmaps = googlemaps.Client(key=mykey)


df = pd.read_csv("NFL Teams.csv")
df.head()


df['Address'] = ""
df['Latitude (generated)'] = 0
df['Longitude (generated)'] = 0


for row in df['Stadium'].iteritems():
    
    geocode_result = gmaps.geocode(row[1])[0]
    
    address = geocode_result['formatted_address']
    lat = geocode_result['geometry']['location'].get('lat')
    lon = geocode_result['geometry']['location'].get('lng')
    
    df.loc[row[0],['Address','Latitude (generated)','Longitude (generated)']] = [address,lat,lon]

df.to_csv("NFL Teams.csv",index = False)


