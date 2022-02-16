# Data Engineering Training

## Start Postgres locally
```
docker run --rm -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v /home/cristiandugacicu/projects/personal/de-zoomcamp/course1b/postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13
```

## Copy google credentials
```
cp secrets/razor-project-339321-3a382dd6a910.json  ~/.google/credentials/google_credentials.json
```

## Change repository owner of all folders
```
chown -R cristiandugacicu:cristiandugacicu dags
```

## Backfill Airflow dags
```
airflow dags backfill -s 2019-1-1 -e 2019-1-31 data_ingestion_gcs_dag3
```

#### ML in GCP using BigQuery

[BigQuery Machine Learning](https://youtu.be/B-WtpB0PuG4)  
[SQL for ML in BigQuery](big_query_ml.sql)

**Important links**
- [BigQuery ML Tutorials](https://cloud.google.com/bigquery-ml/docs/tutorials)
- [BigQuery ML Reference Parameter](https://cloud.google.com/bigquery-ml/docs/analytics-reference-patterns)
- [Hyper Parameter tuning](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-create-glm)
- [Feature preprocessing](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-preprocess-overview)

##### Deploying ML model
[BigQuery Machine Learning Deployment](https://youtu.be/BjARzEWaznU)  
[Steps to extract and deploy model with docker](extract_model.md)  

