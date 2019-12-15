"""This script standardizes the college names across the players and college list.

The players .txt file contains the list of each draft pick from 1998 to 2019.
This file was downloaded from https://www.pro-football-reference.com/ . Additionally,
The College_List.csv file was generated from the "Wikipedia Scraping (1).py script
and contains the list of all the D1-D3 college football programs. Missing college
football programs are added, community colleges and international players are assigned
identified as well. This module saves 3 csv files, 1 with the joined college and player 
list, and one each for the cleaned college and player list.

"""

import pandas as pd
import re
import wikipedia
import bs4 as bs
import urllib.request


fp_players = r"C:\Users\jlosack\Documents\Python\NFL_Team_Comp\Final Scripts\Players.txt"
player_df = pd.read_csv(fp_players)

player_df.rename(columns = {"Cmp":"Passing_Cmps",
                    "Att": "Passing_Atts",
                    "Yds": "Passing_Yds",
                    "TD": "Passing_TDs",
                    "Int": "Passing_Ints",
                    "Att.1": "Rushing_Atts",
                    "Yds.1": "Rushing_Yds",
                    "TD.1" : "Rushing_TDs",
                    "Yds.2" : "Receiving_Yds",
                    "TD.2" : "Receiving_TDs",
                    "Int.1" : "Defensive_Ints"}, inplace = True)

player_df['Player_Code'] = player_df['Player'].apply(lambda x: x.split("\\")[-1])
player_df['Player'] = player_df['Player'].apply(lambda x: x.split("\\")[0])


college_df = pd.read_csv(r"C:\Users\jlosack\Documents\Python\NFL_Team_Comp\Final Scripts\College_List.csv",index_col= 0)
college_df['State'] = college_df['State'].fillna("NA")


#  Standardize the two lists. College list will be changed to match players
#  Create any college records that don't exist in the college list
#  Handle international players by creating new records
#  Any players that don't have a college?
no_colleges = player_df.loc[player_df['College/Univ'].isna(),:].index
player_df.loc[no_colleges]
#  Only 13 players don't have a college assignment. Will just manually look up and fill out:
#  School, City, Conference, Division, Nickname, Stadium, State.
#  People outside of the US will have School = Country, Conference, Division = International
#  School will be filled out on the player list. College list will get the rest of the details

missing_college_list = ["Australia", "Germany", "Colorado Pueblo","Northeastern State",
                        "Louisiana","Regina","Louisiana","Cal Poly", "Missouri Western State", "Georgia State",
                        "UAB", "Bethel (TN)","Northwestern State"]
player_df.loc[no_colleges,"College/Univ"] = missing_college_list


no_match_colleges = player_df.loc[~player_df['College/Univ'].isin(college_df['School']),'College/Univ']
no_match_colleges.head()

#  1749 no matches prior to state expansion
player_df['College/Univ'] = player_df['College/Univ'].replace(r"St\.","State", regex = True)

#  895 no matches after state expansion
player_df.loc[~player_df['College/Univ'].isin(college_df['School']),'College/Univ']

#  College list will sometimes use a "Proper" college name like Alabama State University 
#  while the player list will just use Alabama State
college_df['School Long Name'] = college_df['School']
college_df['School'].replace({r"University of ": "",
                                r"University": ""}, regex = True, inplace = True)
college_df['School'] = college_df['School'].apply(lambda x: x.strip())

#  482 remaining mismatches

missing_college_list = []

def clean_colleges(player_list,college_list):
    """Interactive college name mismatch correction.

    This function will suggest possible college matches for college names that don't match between the list of colleges and
    colleges on the player list. Search for matching colleges by any column name and then enter an index of an already
    existing college to standardize the name across the player list and college list. If no college can be found, then can
    add the college to a missing list which will be manually corrected later. 
    
    Args:
        player_list: A dataframe of player draft picks with a column that has college information
        college_list: A dataframe with the names of colleges that don't match the player_list colleges
    
    Returns:
        No returns. Writes a "College_List_Corrected" csv file with the fixed college names.
        Writes a "Missing_Colleges" csv file with colleges that matches can't be found for.
    """

    while len(player_list.loc[~player_list['College/Univ'].isin(college_list['School']),'College/Univ'].value_counts()) > 0:
        for current_college, number in player_list.loc[~player_list['College/Univ'].isin(college_list['School']),'College/Univ'].value_counts().iteritems():
            print("Current College: " + current_college)
            print("Players:")
            print(player_list.loc[player_list['College/Univ'] == current_college,['Player','College/Univ']].head())
            print("Possible College Matches:")
            print(college_list.loc[college_list['School'].isin(current_college.split()),:])
            print(college_list.loc[college_list['State'].isin(current_college.split()),:])
            search = input("Is further searching needed? (T or F)")
            while search == 'T':
                search_by = input("Search by what column (School, State City etc))?")
                search_for = input("What " + search_by + " to search for?")
                print(college_list.loc[college_list[search_by].str.contains(search_for, regex = True)])
                search = input("Search Again (T or F)")
            print("Current College: " + current_college)
            index = input("Index of college name to replace with player college or None: ")
            if index == "None":
                missing_college_list.append(player_list.loc[player_list['College/Univ'] == current_college,'Player'].index)

                pd.DataFrame(missing_college_list).to_csv("Missing_Colleges.csv")
                continue
            else:
                college_list.at[int(index),'School'] = current_college
                college_list.to_csv("College_List_Corrected.csv")


clean_colleges(player_df,college_df)

#  Handle remaining Colleges Not Found in List. Need to add details to the missing colleges.

missing_college_indices = pd.read_csv("Missing_Colleges.csv")
missing_college_indices = missing_college_indices.drop(columns = 'Unnamed: 0').drop_duplicates()
missing_colleges = player_df.loc[missing_college_indices['0'],'College/Univ']
missing_colleges = pd.DataFrame(missing_colleges)
missing_colleges.columns = ["School"]
college_df = pd.read_csv("College_List_Corrected.csv")
college_df['Country'] = "United States"
college_df = college_df.append(missing_colleges,ignore_index = True)

#  Cal Poly, UAB and Missouri State are definitely in the college list. Handle these by correcting player list

cal_poly_index = player_df.loc[player_df['College/Univ'] == "Cal Poly",:].index[0]
cal_poly_value = player_df.loc[player_df['College/Univ'].str.contains("Cal Poly-"),'College/Univ'].values[0]
player_df.at[cal_poly_index,'College/Univ'] = cal_poly_value

uab_index = player_df.loc[player_df['College/Univ'] == "UAB",:].index[0]
uab_value = player_df.loc[player_df['College/Univ'].str.contains("Ala-Birm"),'College/Univ'].values[0]
player_df.at[uab_index,'College/Univ'] = uab_value

miss_state_index = player_df.loc[player_df['College/Univ'] == "Missouri State",:].index[0]
miss_state_value = player_df.loc[player_df['College/Univ'] == "SW Missouri State",'College/Univ'].values[0]
player_df.at[miss_state_index,'College/Univ'] = miss_state_value

drop_indices = college_df.loc[college_df['School'].isin(["UAB","Cal Poly","Missouri State"])].index
college_df.drop(index = drop_indices,inplace = True)

#  State Augustine's need to be corrected. Northwestern State is also probably already included
staug_index = player_df.loc[player_df['College/Univ'] == "State Augustine's",:].index[0]
staug_value = college_df.loc[college_df['School'].str.contains("Saint August"),'School'].values[0]
player_df.at[staug_index,'College/Univ'] = staug_value

nw_index = player_df.loc[player_df['College/Univ'] == "Northwestern State",:].index[0]
nw_value = college_df.loc[college_df['School'].str.contains("NW State"),'School'].values[0]
player_df.at[nw_index,'College/Univ'] = nw_value

drop_indices = college_df.loc[college_df['School'].isin(["State Augustine's","Northwestern State"])].index
college_df.drop(index = drop_indices,inplace = True)

#  Going to add City, Conference, Division, State, and Country for new colleges
city = ["Hempstead", "Omaha", "Florence", "Montreal", "Oskaloosa","Oroville","Tuscaloosa","Walnut","Arcata",
        "Langston","Jackson","Regina","Sydney","Winnipeg","Jamestown","Lawrenceville","Atlanta","Long Beach",
        "Poplarville","Stuttgart", "McKenzie","London"]
state = ["New York", "Nebraska","Alabama","Quebec","Iowa","California","Alabama","California","California", "Oklahoma",
         "Tennessee","Saskatchewan","New South Wales","Manitoba","North Dakota","Virginia","Georgia","California",
         "Mississippi","Baden-Württemberg","Tennessee","Ontario"]
country = ["United States","United States","United States", "Canada","United States","United States","United States","United States",
           "United States","United States", "United States","Canada","Australia","Canada","United States","United States",
           "United States","United States","United States","Germany","United States","Canada"]
conference = ["CAA","MIAA","Big South","International","HAAC","Community College","SIAC","Community College", "GNAC",
              "Red River","TSAC","International","International","International","GPAC","CIAA","SIAC","Community College",
              "Community College","International","SSAC","International"]
division = ["FCS","DII","FCS","International","NAIA","Community College","DII","Community College","DII","NAIA","NAIA",
            "International","International","International","NAIA","DII","DII","Community College","Community College",
            "International","NAIA","International"]


missing_school = college_df.loc[college_df['School'].isin(missing_colleges['School']),'School']

update_df = pd.DataFrame({'School': missing_school,
            'City': city,
            'State': state,
            'Country': country,
            'Conference': conference,
            'Division' : division})

college_df.update(update_df)

player_df['College/Univ'].replace({"State Paul's":"St. Paul's (VA)"}, inplace = True)
college_df['School'].replace({"State Paul's":"St. Paul's (VA)"}, inplace = True)

#  Checking to make sure everything joins clean
print(player_df.loc[~player_df['College/Univ'].isin(college_df['School']),:])

player_df.rename(columns = {'College/Univ':'School'}, inplace = True)

player_college_join = player_df.merge(college_df, how = 'left',on = 'School')

player_college_join.to_csv("Player and School List.csv")
college_df.to_csv("School List.csv")
player_df.to_csv("Player List.csv")