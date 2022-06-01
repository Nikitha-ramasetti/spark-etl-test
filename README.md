# Data Warehouse Challenge - Spark Networks Services


## Description

This project is a ETL/ELT challenge (creating a data pipeline) and data analysis executed with Python, SQL and git skills.


To begin with, I created a data collector python script, to extract information by making requests from the data source in a RestAPI, which returns a JSON file.
Extracted file was normalized and created as DataFrame for further manipulation and casting into datatypes.
The PII handling was considered to remove the sensitive and personal information.
Finally connection to the database was established to load the transformed information into the database.


#### Project 
About technologies, I have used: PostgreSQL (DBeaver) to build the project. 

For the project and workflow, I have created a python_src folder with consists of 3 files:
- db_connect.py
- extract_transform.py
- main.py

#### Model

**image/model_rel.png** file to view the dimensional model\
Within this project, I have proposed database model to build the relationship with schema:

* dimensional table stored about user entities.
* fact table with users subscriptions.
* fact table stored about every message sent and received to users entities by a foreign key.



#### SQL queries to answer

Use **sql_queries/sql_test.sql** file to run the queries in the new database.

I have used DBeaver/PgAdmin4 query tool by connecting to the database for testing the queries and analyses for below questions:

- How many total messages are being sent every day?
- Are there any users that did not receive any message? 
- How many active subscriptions do we have today?
- Are there users sending messages without an active subscription? (some extra context for you: in our apps only premium users can send messages).
- Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses? How to monitor it (SQL query)? Please explain how do you suggest to handle with this noisy data?




### Setup

Clone for Github repository, or can download the repository and unzip it.\
https://github.com/Nikitha-ramasetti/spark-etl-test

```sh
git clone <repo_url>
```

### Requirements:

A requirements.txt is available in the python folder. (**python_src/requirements.txt**)


```sh
numpy==1.19.5
pandas==1.1.5
psycopg2==2.9.3
requests==2.27.1
SQLAlchemy==1.4.36
```


### Virtual environment

* Create Python Virtual Environment 
* Install requirements
* Run ETL (main.py) script

```sh
python3 -m virtualenv venv
source venv/bin/activate  ( #Activate virtual environment)
python -m pip install -r requirements.txt
python python_src/main.py
```

#### Credentials:


```sh
{
    'host': 'localhost',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5433,
    'database' : 'postgres'
}
```


## Author

**Nikitha Ramasetti** <nikitha.shruthi@gmail.com>
