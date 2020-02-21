##### Udacity Data Engineering Nanodegree
<img alt="" align="right" width="150" height="150" src = "./image/cassandraLogo.png" title = "cassandra logo" alt = "Cassandra logo">   

</br>
</br>

# Project 2 : Data Modeling with Cassandra




About ETL and event data modeling to create a non-relational database and ETL pipeline for a music streaming application. We define queries and tables for a database built using Apache Cassandra.  


### Table of contents

   - [About the project](#about-the-project)
   - [Purpose](#purpose)
   - [Getting started](#getting-started)
   - [Ressources](#ressources)
       - [Dataset EventData](#dataset-eventdata)
       - [Tools and Files](#tools-and-files)
   - [Worflow](#worflow)
      - [Modeling your NoSQL database](#modeling-your-nosql-database)
      - [Build ETL pipeline](#build-etl-pipeline)
   - [Workspace](#workspace)
      - [My environnemets](#my-environements)
      - [Discuss about the database](#discuss-about-the-database)
      - [UML diagram](#uml-diagram)
      - [Chebotko diagram](#chebotko-diagram)
      - [Queries](#queries)
      - [Web-links](#web-links)
<!--CACHER-->

## About the project

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.  
They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions.
* Create a database for this analyse
* Create Tables in Apache Cassandra
* Insert data
* Test the database by running queries

## Purpose

The purpose of this project is to apply data modeling with Apache Cassandra and build an ETL pipeline using Python.  
We will need to creating tables in Apache Cassandra to run queries.

## Getting started

* Start Cassandra and Jupyter Notebook
* Open `Project2_cassandraModeling.ipynb`
* Make sure you have drop the tables and closed (statements at the end of the file) the connection before leaving `Project2_cassandraModeling.ipynb`.

## Ressources

### Dataset EventData
 The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:  

```python
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

### Tools and Files
To get started with the project, go to the workspace on the next page, where you'll find the project template (a Jupyter notebook file). You can work on your project and submit your work through this workspace.

The project template includes one Jupyter Notebook file, in which:
   * you will process the `event_datafile_new.csv` dataset to create a denormalized dataset
   * you will model the data tables keeping in mind the queries you need to run
   * you have been provided queries that you will need to model your data tables for
    * you will load the data into tables you create in Apache Cassandra and run your queries

## Worflow

### Modeling your NoSQL database
* Design tables to answer the queries outlined in the project template
* Write Apache Cassandra `CREATE KEYSPACE` and `SET KEYSPACE` statements
* Develop your `CREATE` statement for each of the tables to address each question
* Load the data with `INSERT` statement for each of the tables
* Include `IF NOT EXISTS` clauses in your `CREATE` statements to create tables only if the tables do not already exist. We recommend you also include `DROP TABLE` statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
* Test by running the proper select statements with the correct `WHERE` clause
### Build ETL pipeline
* Implement the logic in section Part I of the notebook template to iterate through each event file in `event_data` to process and create a new CSV file in Python
* Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and `INSERT` statements to load processed records into relevant tables in your data model
* Test by running `SELECT` statements after running the queries on your database

## Workspace

### My environnemets

* I create an anaconda environemet 'cassand3' with python=3.6
* check the java version
  * `java --version`
* Install Pandas, numpy, cassandra driver
  * `pip install pandas`
  * `pip install cassandra-driver`
* Install cassandra
  *  https://www-us.apache.org/dist/cassandra/3.11.5/apache-cassandra-3.11.5-bin.tar.gz
  * cd ~/Telechargement
  * chmod 755
  * tar -xvzf apache-cassandra-3.11.5.tar.gz
  * sudo mv apache-cassandra-3.11.5 /opt
  * sudo mv apache-cassandra-3.11.5 cassandra
  * sudo mkdir /var/lib/cassandra/
  * sudo mkdir /var/lib/cassandra/commitlog
  * sudo mkdir /var/lib/cassandra/data
  * sudo mkdir /var/log/cassandra/
  * sudo chown -R <USER> /var/lib/cassandra/ /var/log/cassandra/  *   
  _modifier le path dans .zshrc_
  * export CASSANDRA_HOME=/opt/cassandra
  * export PATH=$PATH:$CASSANDRA_HOME/bin:$CASSANDRA_HOME/sbin
* To work each time
  *  conda activate cassand3
  * cd /opt/cassandra
  * bin/cassandra -f #start cassandra
* /opt/cassandra/bin/nodetool status # cassandra is running?
  * start anaconda-navigator for Jupyter notebook

### Discuss about the database



### UML diagram  
![diagram UML](./image/erd_project2.png)  
To take stock of the data, I've drawn a UML diagram that allows me to identify 3 entities and their attributes. 

### Chebotko diagram
![Mapping Rules](./image/mappingRules.png)  


From this point, I used the method of the diagram chebokto to answer the 3 queries by looking for each time **the attributes one possesses, query predicate (k) and ordering attributes (C)** for the primary key and **desired attributes** for what we looking for.  


![diagram chabokto](./image/quer1.png)  


Then, it helps me to design every query    


![diagram chabokto](./image/query1_table.png)  

### Queries
```sql
# create table for the first query
query = "CREATE TABLE IF NOT EXISTS song_by_session"
query = query + "(sessionId INT, itemInSession INT, artist VARCHAR, song VARCHAR, length DECIMAL, PRIMARY KEY(sessionId, itemInSession))"
```
```python
# insert data
 file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_by_session(sessionId, itemInSession, artist, song, length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
```
```python
# Query
query = "SELECT * FROM song_by_session WHERE sessionId = 338 AND itemInSession = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist,"---", row.song,"---" ,row.length)
```
### Web-links

[Using the Chebotko Method](https://fr.slideshare.net/ArtemChebotko/using-the-chebotko-method-to-design-sound-and-scalable-data-models-for-apache-cassandra)  
[Rigorous Cassandra Data Modeling](https://fr.slideshare.net/ArtemChebotko/rigorous-cassandra-data-modeling-for-the-relational-data-architect)  
[Data Modeling in Cassandra](https://www.baeldung.com/cassandra-data-modeling)  
[Data Types](http://cassandra.apache.org/doc/latest/cql/types.html)  
[The Simplest Way to Visualize Concept](https://creately.com/)  
[Cassandra Performance](https://www.scnsoft.com/blog/cassandra-performance#data-modeling)  
[Designing a Cassandra Data Model](https://shermandigital.com/blog/designing-a-cassandra-data-model/)  
