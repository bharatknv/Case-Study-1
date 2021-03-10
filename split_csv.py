import csv
import pandas as pd
import datetime

def change_date_format(arr):
    dates = []
    for date in arr:
        day, month, year = date.split("-")
        
        switch = {
            'Jan': '01',
            'Feb': '02',
            'Mar': '03',
            'Apr': '04',
            'May': '05',
            'Jun': '06',
            'Jul': '07',
            'Aug': '08',
            'Sep': '09',
            'Oct': '10',
            'Nov': '11',
            'Dec': '12'
        }

        year = "20" + year

        month = switch.get(month, None)

        dates.append("-".join([year, month, day]))
    
    return dates


def generate_unique_ids(arr):
    id_dict = {}

    id = 1

    for i in arr:
        id_dict[i] = id
        id += 1
    
    return id_dict

if __name__ == "__main__":
    
    df = pd.read_csv("Google playstore data_Cleaned_UNIQUE.csv")

    df["Last_Updated"] = change_date_format(df["Last_Updated"]) # Fix the date formatting in the csv

    required_columns = ["Category", "Genres", "Type", "Android_Ver", "Content_Rating"]
    other_columns = ["App", "Rating", "Reviews", "Size", "Installs", "Price", "Last_Updated", "Current_Ver"]

    walmart_fact_table = pd.DataFrame()

    ids = {} # Dictionary where the column names are the keys and the value will be a dictionary 
             # where the unique values of that column will be the key and an id will be the value
    
    data_frames = {} # Dictionary of separate data frames for each column + id for each value in that column

    for column in required_columns:
        ids[column] = generate_unique_ids(df[column].unique())

        data_frames[column] = pd.DataFrame(df[column])
        id_list = []

        for value in data_frames[column][column]:
            #print(data_frames[column].head())
            #print(value)
            id_list.append(ids[column][value])

        data_frames[column][column + "_id"] = id_list
        data_frames[column] = data_frames[column][[column + "_id", column]]

        walmart_fact_table[column + "_id"] = id_list

        data_frames[column] = pd.DataFrame.drop_duplicates(data_frames[column])
        data_frames[column].to_csv("output/" + column + ".csv", index = False)
    
    for column in other_columns:
        walmart_fact_table[column] = df[column]

    walmart_fact_table.to_csv("output/walmart_fact_table.csv", index = False)