"""This script transforms data into the format requir. by Tableau for map paths.

Tableau recommends that data be formatted a specific way when maps with paths are
created. This script manipulates the "Player_Schol_NFL_LATLON_List" csv file
into the format suggested here: https://help.tableau.com/current/pro/desktop/en-us/maps_howto_origin_destination.htm
Additionally, to have lines that arc across the map instead of straight lines, the
great circle formula is used to generate additional intermediate points between the
player's college location and NFL team that drafted them. A Path's .csv file is saved and is
used as a datasource in the Tableau visualization. Additionally, draft year
is added to the player list and the "Player_School_NFL_LATLON_Year.csv" file is saved that 
will be used in the Tableau visualization as well.

"""


import pandas as pd
import great_circle_calculator.great_circle_calculator as gcc
import numpy as np

#  This controls how many intermidate points to generate to form the arcs
num_of_ipoints = 100  

df = pd.read_csv(r"Player_School_NFL_LATLON_List.csv", index_col=0)


def calc_intermediate(origin_point, destination_point,num_of_points):
    """Takes an origin/destination lat/lon pair and generates intermediate points.

        Args:
            origin_point: a list-like item where the 0th index is lat and 1 index is lon
            destination_point: a list-like item where 0th index is lat, and 1 index is lon
            num_of_points: integer that specifies how many intermediate points to calculate
        Returns:
            List of lat/lon tuples of length num_of_points
    """
    
    origin_point = tuple([origin_point[1],origin_point[0]])
    destination_point = tuple([destination_point[1],destination_point[0]])
    i_points = []

    for frac in np.linspace(1/num_of_points,1,num_of_points, endpoint=False):
        new_ipoint = gcc.intermediate_point(origin_point,destination_point, frac)
        i_points.append(new_ipoint)
    
    return i_points


first_row = True

#  Forgot to add draft year column in previous cleaning
df['Draft_Year'] = 0
for index, row in df.iterrows():
    if first_row == True:
        current_year = 2019
    #  Player list is in draft pick order beginning with 2019,
    #  so after pick ~256 happens, draft pick 1 from the prior year's
    #  draft is the next row.
    elif (first_row == False) & (row['Pick'] - df.at[index - 1,'Pick'] < 1):
        current_year -= 1

    df.at[index,'Draft_Year'] = current_year

    paths = pd.DataFrame(columns = ["Origin_Destination","Team","Path ID","Path Latitude","Path Longitude"])
    pathid = row['School'] + "_"+ str(row['Draft_Year']) + "_"+ str(row['Pick']) + "_"+ row['Tm']
    college_coordinates = row[['College Latitude','College Longitude']]
    nfl_coordinates = row[['NFL Latitude','NFL Longitude']]
    df_index = [index]*num_of_ipoints

    intermediate_rows = pd.DataFrame(calc_intermediate(college_coordinates, nfl_coordinates, num_of_ipoints),
                                    columns = ['Path Longitude','Path Latitude'], index = df_index)


    origin_row = pd.DataFrame({"Origin_Destination":"Origin",
                    "Team": row['School'],
                    "Order of Points": 1,
                    "Path ID": pathid,
                    "Path Latitude": college_coordinates[0],
                    "Path Longitude": college_coordinates[1]}, index = [index])

    origin_row_path = origin_row.copy()
    origin_row_path['Origin_Destination'] = 'Path'
    origin_row_path['Order of Points'] = 2

    intermediate_rows['Origin_Destination'] = "Path"
    intermediate_rows['Team'] = row['School'] + '_' + row['Tm']
    intermediate_rows['Order of Points'] = list(range(3,num_of_ipoints+3,1))
    intermediate_rows['Path ID'] = pathid
    
    destination_row = pd.DataFrame({"Origin_Destination":"Destination",
                    "Team": row['Tm'],
                    "Order of Points" : num_of_ipoints + 3,
                    "Path ID": pathid,
                    "Path Latitude": nfl_coordinates[0],
                    "Path Longitude": nfl_coordinates[1]}, index = [index])
    
    paths = pd.concat([paths,origin_row,intermediate_rows,destination_row])
    
    fppath = "Path_Final.csv"


    try:
        with open(fppath, 'a') as f:
            paths.to_csv(f, header=first_row, index=True)
            print('Success: ' + str(index))
    except:
        print('Unable to Write CSV')
    first_row = False

fpplayerlist = "Player_School_NFL_LATLON_Year.csv"
df.to_csv(fpplayerlist, index=True)

