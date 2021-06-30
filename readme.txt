ETL processing for extracting data from csv files and loading into Mysql Database.
Purpose: To Extract, transform and load data from csv files to RDBMS. It will load data for recipes, interactions (date,rating and comments given for users to one or many recipes) and nutrition values of each recipe 
including calories and personal daily values of fats, proteins and so on.    
language: Python 3.8.5
Database: Mysql 8.0.25
Database Connection library: pymysql
ETL python library: petl


Folders:
    CSV: All the csv files with raw and initally processed data-. Files are not included due to git' limits of space 
    
    Database: Python Package with class for connecting to Mysql 
    |--- Connection.py: Class for Connect and execute DML commands with Mysql using pymysql 
    
    file_management: Python Package with classes for Extract, validate, trandform and load data from files to Mysql
    |--- RecipeFileManager.py: 
    |--- InteractionFileManager.py
    
    venv: python virtual Environment
    
    mysql_scripts: contains SQL Scripts files with commands for creating database, user and tables for data populating (loading)
    |--- tables.sql. DDL for creating tables
    |---database.sql. Commands for creating the database and user

    requirements.txt: File to include libraries and dependencies for the project.

    main.py. File with code to invoke the classes with its methods in order to execute the etl process.

Pending Improvements:
    * Multilple Date format parsing once the data is extracted with petl.
    * Process Log and Detailed process log (Rows validated, rows ommited, time elapsed, etc)
    * Simpler and better extraction of recipe's nutrition values.
    * Tokenized items.     