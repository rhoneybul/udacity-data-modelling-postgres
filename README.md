## Data Modeling with Postgres

This project aims to create an ETL pipeline for 'Sparkify', to enable the analysis of song listens. At the moment, there is no easy way to query the data, as it resides in a directory of log files in json format. The project takes these log files, and creates and populates postgres tables, in order to enable further analyis. 

### Table Structure

The tables are structured as a star schema, with a dimension table (song plays), as well as fact tables, for users, songs, artists and times.

### Project Structure

The project consists of the following scripts;

* sql_queries.py - This script contains the sql queries required for creating tables, inserting values, dropping tables, and selecting songs.
* create_tables.py - This script imports the relevant create and drop queries from sql_queries.py, and creates the tables neccessary for the particular database.
* etl.py - This script takes the log files, and song files from the ./data directory, reads each log / song, and creates the relevant inserts to create the tables.

### Running the Scripts

To run the ETL pipeline end to end, you need to run the scripts in the following manner;

1. create_tables.py - This should be run first to create the neccessary tables.
2. etl.py - This should then be run to populate the tables created in the previous step.

To then ensure that the tables are created, and subsequently populated correctly, you can run test.ipynb to view the tables created and data within the tables.

