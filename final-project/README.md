# GitHub events metrics (WIP)

This is a Data Engineering project which creates a data pipeline for monitoring GitHub events in realtime (updates every 1 hr)
Github updates the dataset to the last hour.
Details: https://www.gharchive.org/

### Steps:
 - [x] data analysis to research which data to monitor
 - [x] pipeline to load files from GitHub API and store them in Google storage
 - [x] pipeline to import all files from Google store to Google BigQuery DB
 - [x] pipeline to transform from data lake to a Data Warehouse using clean data
 - [x] pipeline to transform from Data Warehouse raw tables to aggregated data views
 - [x] pipeline to create realtime visualization dashboards
 - [ ] pipeline to perform bigdata processing
 - [ ] review and cleanup pipelines
 - [ ] deploy to cloud
 - [ ] create CI/CD

### Data analysis

See the analysis in jupyter notebook: [LINK]

- loaded data for 1 hour from GitHub API
- cleaned up the data and created a DataFrame
- decided to create dashboards for following data:
    - Count of event types
    - Distribution of commit count for PushEvents
    - Most common words in commits

These 3 dashboards will be displayed in the final visualizations once the data pipeline is complete.    

### Airflow

Airflow has a DAG with multiple jobs: 
    - download github file every hour
    - convert from .json.gz to .parquet
    - upload data to Google Storage
    - transfer data from Google Storage to Google Bigquery
    - delete local temp files

#### Run locally:    
```
cd airflow
docker-compose up
```

### DBT
DBT is used to create BigQuery views of aggregated data.

#### Run locally:
```
docker run --rm -it \
    -v $PWD:/dbt \
    -v ..../google_credentials.json:/dbt/google_credentials.json \
    -v profiles.yml:/root/.dbt/profiles.yml \
    davidgasquez/dbt:latest dbt run --profiles-dir /dbt --full-refresh
```


### Visualizations:

Visualizations are done using Google Data Studio and they update in realtime (every hour once data is ingested).

https://datastudio.google.com/s/i0zQ5DwgbuA