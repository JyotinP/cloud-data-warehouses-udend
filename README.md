# Introduction and Purpose

As a startup in the music streaming industry, we want want to analyze the data which we've been collecting on songs and user activity. The   analytics team is particularly interested in understanding what songs users are listening to. We want to provide data in a format which makes it easier for them to do so.

First we'll define and create new fact and dimension tables according to the star schema, which makes it easy and efficient to execute analytical join queries.

Then we'll build an ETL pipeline to first load data from S3 into staging tables on Redshift. And then process and insert that data into our final analytics tables on Redshift.

# Database schema design and ETL pipeline

'songplays' is our Fact Table. This contains records in log data associated with song plays. Data is filtered to contain only information associated with 'NextSong' page. We've implemented auto incremental Identity key value for 'songplay_id'. 'start_time' is the sortkey as this column will be commonly used in WHERE clauses. 'song_id' is the distkey as as this column will be commonly used to join other tables.

'users' is our Dimension Table. This contains records of all the users in the app. It contans name of the user, gender and level of subscription of the app. 'user_id' is the sortkey as this column will be commonly used in WHERE clauses.

'songs' is our Dimension Table. This contains records of all songs in the music database. It contains song title, year of release and duration. 'song_id' is the sortkey and distkey as this column will be commonly used in WHERE clauses and in JOINs. 

'artists' is our Dimension Table. This contains records of all artists in music database. It contains name of the artist and location details. 'artist_id' is the sortkey as this column will be commonly used in WHERE clauses.

'time' is our Dimension Table. This contains records of all timestamps of records in songplays broken down into specific units.  
Date/time data is broken down into hour, day, week, month, year, weekday columns. 'start_time' is the sortkey as this column will be commonly used in WHERE clauses.

# How to run python scripts and description of the files

'sql_queries.py' is where we have defined all the table drop/create queries and copy/insert statements.

'create_tables.py' has all the functions which executes the create table queries from the 'sql_queries.py'. Executing this will establish connection with the Redshift and create the defined tables.

'etl.py' is where we process data from all the log and song json files with the ETL pipeline developed in 'etl.ipynb' file.

'dwh.cfg' is where we store informations about credentials, cluster specifications, etc.