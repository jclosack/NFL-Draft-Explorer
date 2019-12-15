"""Additional cleaning and final merge of college, nfl and player data.

This script performs additional cleaning to standardize values in a variety
of columns. After cleaning is completed, a csv file is written with all Player,
NFL, and college data merged. This csv file is loaded into Tableau to generate
lat/lon coordinates for each college city, state and country. These coordinates
are pasted into a csv, read into this file and rejoined to the larger dataframe.
"Player_School_NFL_LATLON_List.csv" is written.

"""

import pandas as pd

fp_players = r"Final Scripts\Player and School List.csv"

df = pd.read_csv(fp_players,index_col=0)
df.head()
df.rename(columns = {"Unnamed: 0.1": "College_List_Index"}, inplace = True)



for label, content in df.iteritems():
    df[label].value_counts()

#  Tm column acronyms need to match the nfl data. #Players share names, need to be careful on handling players.
#  Can drop "Unnamed: 28". Conference Name, joined FBS, first played, college city needs cleaning.

df.drop(columns = "Unnamed: 28",inplace = True)

#  All 3 of these have a [XX] pattern. split these columns on [ and take the first element
bracket_columns = ['Conference','JoinedFBS','FirstPlayed','City']
df[bracket_columns] = df[bracket_columns].apply(lambda x: x.str.split("[").str.get(0))

#  Independent and FBS Indenpendent exist in the conference column. What's the difference?
df.loc[df['Conference'].isin(["Independent","FBS Independent"]),:].groupby(['School','Conference']).size()

#  All values except North Dakota should be FBS Independent
df.loc[df['School']== "North Dakota",'Conference'] = "FCS Independent"
df.loc[df['Conference']== "Independent",'Conference'] = "FBS Independent"

#  Conferences by division to check for similar conference names across divisions
df.groupby(['Conference','Division']).size()

#  MAC and MIAA are both DII and DIII conferences

df['Conference'] = df['Conference'].mask((df['Division'] == "DIII") & (df['Conference'] == "MAC"),"MAC (DIII)")
df['Conference'] = df['Conference'].mask((df['Division'] == "DIII") & (df['Conference'] == "MIAA"),"MIAA (DIII)")

#  NFL team information read in. This list was compiled manually
nfl = pd.read_csv("NFL Teams.csv")

#  What abbreviations don't match from the player list
df.loc[~df['Tm'].isin(nfl['Abbr']),:].groupby('Tm').size()
df['Tm'].replace({
    "SFO": "SF",
    "NWE": "NE",
    "GNB":"GB",
    "WAS": "WSH",
    "KAN": "KC",
    "TAM": "TB",
    "STL": "LAR",
    "NOR": "NO",
    "SDG": "LAC"}, inplace = True)

df.loc[~df['Tm'].isin(nfl['Abbr']),:].groupby('Tm').size()

nfl.rename(columns = {"Abbr": "Tm",
                      "Latitude (generated)":"NFL Latitude",
                      "Longitude (generated)": "NFL Longitude",
                      "Conference": "NFL Conference",
                      "Division":"NFL Division",
                      "Stadium": "NFL Stadium",
                      "City" : "NFL City"}, inplace = True)

new_df = df.merge(nfl, how = 'left', on = 'Tm')

new_df.to_csv("Player_School_NFL_List.csv")

#  The "Player_School_NFL_List" csv file is loaded into Tableau to generate latitutde and longitudes 
#  for every college city.
#  NFL stadium coordinates are geocoded using google maps api in nflstadium_geocode.py
#  Unrecognized lat's and lon's are minimal and will be manually corrected here

tb_df = pd.read_csv(r"Final Scripts\Tableau_Generated_Coords.csv")

#  Fill null latitidue and longitude coordinates for 4 cities. Normal Alabama, storrs connecticut, 
#  urbana-champaign illinois, Ettrick viriginia. Coordinates retrieved by manually searching Google Maps

missing_locations = tb_df.loc[tb_df.isnull().any(axis = 1),['City','State']]

coords = pd.DataFrame({'Normal':[34.784074,-86.574921],
'Storrs': [41.8059851,-72.2684075],
'Urbana-Champaign': [40.1153943,-88.2281516],
'Ettrick':[37.2444507,-77.446483]}).T
coords.columns = tb_df.columns[-2:]
coords = coords.reset_index()
coords.rename(columns = {'index':'City'}, inplace = True)

tb_df.update(coords, overwrite = False)


df = pd.read_csv(r"Final Scripts\Player_School_NFL_List.csv", index_col = 0)
df = df.merge(tb_df, how = 'left', on = ['City','State','Country'])


df.rename(columns = {'Latitude (generated)' : 'College Latitude',
                     'Longitude (generated)': 'College Longitude'}, inplace = True)

df.to_csv("Player_School_NFL_LATLON_List.csv")