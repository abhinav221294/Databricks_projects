# Databricks Case Study Formula 1

## ER Diagram for Case Study

<img width="1006" height="438" alt="ER_Diagram" src="https://github.com/user-attachments/assets/07048b1c-c5e9-43c2-ac18-4e538a789f43" />

<br><br>  <!-- Add extra <br> for more vertical space -->

<img width="1157" height="351" alt="ER_Diagram2" src="https://github.com/user-attachments/assets/b48e466d-7e01-4e3a-9eec-41d812df5a31" />



<br><br>  <!-- Add extra <br> for more vertical space -->

1) Formula 1 Datasources and Datasets
2) Prepare the data for our Project.
3) We would be going through the project requirements
4) We would be discussing about the complete solution architecture.

Terminologies Used in Formula F1 car Race.

F1 race happens once in a year and it has various seasons similar to what we have in IPL.
Races happens on weekend
Race Circuit - One race at a time
 10 Teams participate in F1 race and there are 2 drivers
2 Practice Sessions  which happens on Fri and Sat

Laps 50-70 and it often depends on the length of the circuit
Pit Stops where driver stops to change the tryres.
At the last, we get driver and constructor standings.

Data Overview
ER Diagram and Database user guide.

Project Requirements.
Data Ingestion -> Ingest 8 files into the datalake, Incremental Load
Data Transformation -> Join the key info required for reporting to create a new table, Join the key info required for Analysis to create a new table
Reporting -  Data related to driver and constructor standings.
Analysis - Dominant Drivers, Dominant Teams, Visualize the output by creating Databrick Dashboards
Scheduling Requirements -> Scheduled it for every Sunday 10 pm, Monitor the pipelines, To rerun failed pipelines, setup alerts on failures.
Non Functional Requirements -> Delete individual records, ability to see the history.

Solution flow

Ergast API -> Data Lake -> Ingest -> ADLS Ingested Layer -> Transform -> Analyze-> Dashboards

Azure Data factory to automate the above pipeline for scheduling

Data Ingestion

Requirements

Ingest all 8 files into datalake.
schema applied to the ingested data
audit columns like the source from where the data is coming and audit date
columnar format
analyze the ingested data using SQL
Ingested data must be able to handle the incremental load.

Data Ingestion overview

csv,json,xml -> read data -> transform data -> write data -> parquet,avro,delta,jdbc
read -> dataframe reader api
transformation -> dataframe api 
write data-> DataFrameWriter API


Constructor file

Steps

JSON -> Read Data-> Transform the Data -> Write Data -> Parquet
         


         constructorid               constructor_id
         constructorRef              constructor_ref

         name                          name
         nationality                   nationality
         url                            drop url
                                        Ingestion date



