# Project: Data Modeling with Postgres

## Overview

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

## Project files

`Project_1B_ Project_Template.ipynb` notebook containing the project code

`docker-compose.yml` docker-compose file to spin up a local cassandra single node cluster

## Dependencies

- Python 3.6 or newer
- `virtualenv` python package to create venv dir
- `docker` and `docker-compose`

## Run

### Create and activate virtual environment
```shell
virtualenv venv
source venv/bin/activate 
```

### Install dependencies
```shell
python -m pip install -r requirements.txt
```

### Bootstrap Cassandra cluster in docker
```shell
docker-compose up -d
```

### Start and open "Project_1B_Project_Template.ipynb" file, run it
```shell
jupyter notebook
```