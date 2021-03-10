# Case Study

## Step 1 - Cleaning the data

- Done by Gourav. TODO fill in the steps

## Step 2 - Splitting the csv file

- Run split.py
    ```Python
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
    ```

## Step 3 - Creating the database

```SQL
CREATE DATABASE case_study_1;

USE case_study_1;
```

## Step 4 - Creating the tables in Hive

```SQL
CREATE TABLE Category(category_id int, category string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE Genre(genre_id int, genre string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE Type(type_id int, type string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE Android_Version(android_version_id int, android_version string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE Content_Rating(content_rating_id int, content_rating string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE play_store(category_id int, genre_id int, type_id int, android_version_id int, content_rating_id int, app string, rating double, reviews int, size double, installs bigint, price double, last_updated date, current_version string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");
```

## Step 5 - Loading the data

```SQL
LOAD DATA LOCAL INPATH '/home/cloudera/Documents/Case\ Study/Category.csv' INTO TABLE Category;

LOAD DATA LOCAL INPATH '/home/cloudera/Documents/Case\ Study/Genres.csv' INTO TABLE Genre;

LOAD DATA LOCAL INPATH '/home/cloudera/Documents/Case\ Study/Type.csv' INTO TABLE Type;

LOAD DATA LOCAL INPATH '/home/cloudera/Documents/Case\ Study/Android_Ver.csv' INTO TABLE Android_Version;

LOAD DATA LOCAL INPATH '/home/cloudera/Documents/Case\ Study/Content_Rating.csv' INTO TABLE Content_Rating;

LOAD DATA LOCAL INPATH '/home/cloudera/Documents/Case\ Study/walmart_fact_table.csv' INTO TABLE play_store;
```

## Step 6 - The queries

1. App count by categories
    
    ```SQL
    SELECT c.category, COUNT(p.category_id) as num_apps
    FROM Category c JOIN play_store p
    ON c.category_id = p.category_id
    GROUP BY c.category;
    ```
2. App count by genres
    
    ```SQL
    SELECT g.genre, COUNT(p.genre_id) as num_apps
    FROM Genre g JOIN play_store p
    ON g.genre_id = p.genre_id
    GROUP BY g.genre;
    ```
3. Number of installs from individual genres
    
    ```SQL
    SELECT g.genre,COUNT(ps.installs)
    FROM genre g join playstore ps
    ON g.genre_id=ps.genre_id
    GROUP BY g.genre;
    ```
    
4. Top 3 categories based on installs
    
   ```SQL
   SELECT c.category,SUM(ps.installs) as ca
   FROM Category c join play_store ps
   ON c.category_id=ps.category_id
   GROUP BY c.category
   ORDER BY ca DESC
   LIMIT 3;
   ```

    
5. Top 20 apps of Game category
    ```SQL
    SELECT c.category,p.app  
    FROM category c JOIN play_store p 
    ON c.category_id=p.category_id 
    WHERE c.category="GAME" 
    ORDER BY p.installs DESC 
    LIMIT 20;
    ```
    
6. Top 20 apps of Communication category
    ```SQL
    SELECT c.category,p.app  
    FROM category c JOIN play_store p 
    ON c.category_id=p.category_id 
    WHERE c.category="COMMUNICATION" 
    ORDER BY p.installs DESC 
    LIMIT 20;
    ```
    
7. Top 20 apps of Social category
    ```SQL
    SELECT c.Category AS Category, p.App AS App_Name, p.installs AS No_Of_Installs 
    FROM Category c JOIN play_store p ON c.category_id = p.category_id 
    WHERE c.Category = 'SOCIAL' 
    ORDER BY No_Of_Installs DESC LIMIT 20;
    ```
    
8. Top 10 app category for teens
    ```SQL
    SELECT c.category AS Category, COUNT(p.App) AS No_Of_Apps 
    FROM Category c, play_store p, Content_Rating cr 
    WHERE p.content_rating_id = cr.content_rating_id 
    AND c.category_id = p.category_id 
    AND cr.content_rating = 'Teen' 
    GROUP BY c.category 
    ORDER BY No_Of_Apps DESC LIMIT 10;
    ```
