"""This script scrapes wikipedia and saves a csv with college football info.

College football program information for Division 1 - 3 schools are scraped
from Wikipedia and saved to a single csv file. Cleaning is done to standardize
column names across the tables and format the Division field.
"""

import bs4 as bs
import urllib.request
import pandas as pd
import re

url_list = ["https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_FBS_football_programs",
"https://en.wikipedia.org/wiki/List_of_NCAA_Division_I_FCS_football_programs",
"https://en.wikipedia.org/wiki/List_of_NCAA_Division_II_football_programs",
"https://en.wikipedia.org/wiki/List_of_NCAA_Division_III_football_programs"]

college_list = []

for url in url_list:
    print(url)
    source = urllib.request.urlopen(url).read()
    soup = bs.BeautifulSoup(source,'lxml')
    column_headings = []
    current_division = url.split("/")[-1]


    table = soup.find("table")

    
    table_header = table.find_all("th")
    for column_name in table_header:
        column_name_clean = re.sub(r"\n","",column_name.text)
        column_headings.append(column_name_clean)
        

    colleges_df = pd.DataFrame(columns = column_headings)

    table_rows = table.find_all("tr")
    for row in table_rows:
        table_cells = row.find_all("td")
        row = pd.DataFrame([i.text for i in table_cells]).T
        try:
            row.columns = column_headings
        except ValueError as er:
            continue
        colleges_df = colleges_df.append(row, ignore_index = True)
    
    colleges_df['Division'] = current_division
    colleges_df.replace(r"\n","", regex = True, inplace = True)
    college_list.append(colleges_df)

complete_colleges = pd.concat(college_list, ignore_index = True)

complete_colleges['Conference'].fillna(complete_colleges['CurrentConference'],inplace = True)
complete_colleges.drop(columns = 'CurrentConference',inplace = True)

complete_colleges['State'].fillna(complete_colleges['State[1]'],inplace = True)
complete_colleges['State'].fillna(complete_colleges['State/Province'],inplace = True)
complete_colleges.drop(columns = ['State[1]','State/Province'],inplace = True)

complete_colleges['School'].fillna(complete_colleges['Team'],inplace = True)
complete_colleges.drop(columns = 'Team',inplace = True)

def clean_division(division):
    """Manipulates URL strings and returns a college football division string.
    Args:
        division: String in the form of:
        "List_of_NCAA_Division_III_football_programs".
    Returns:
        String representing the new college division form: DII, DIII or the
        argument unmanipulated.
    """
    try:
        if division.split("_")[4] == "I":
            return division.split("_")[5]
        else:
            return "D" + division.split("_")[4]
    except IndexError as e:
        return division

complete_colleges['Division'] = complete_colleges['Division'].apply(clean_division)
complete_colleges.to_csv('College_List.csv')
