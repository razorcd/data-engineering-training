# GitHub events metrics (WIP)

This is a Data Engineering project which creates a data pipeline for monitoring GitHub events in realtime (updates every 1 hr)

###Steps:
 - [x] data analysis
 - [ ] pipeline to load files from GitHub API and store them in Google storage
 - [ ] pipeline to import all files from Google store to Google BigQuery DB
 - [ ] pipeline to transform from data lake to a Data Warehouse using clean data
 - [ ] pipeline to create realtime visualization dashboards
 - [ ] pipeline to perform bigdata processing

### Data analysis

See the analysis in jupyter notebook: [LINK]

- loaded data for 1 hour from GitHub API
- cleaned up the data and created a DataFrame
- decided to create dashboards for following data:
    - Count of event types
    - Distribution of commit count for PushEvents
    - Most common words in commits

These 3 dashboards will be displayed in the final visualizations once the data pipeline is complete.    