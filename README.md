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
