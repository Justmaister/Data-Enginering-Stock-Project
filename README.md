# Data-Enginering-Stock-Project

## Project Description
- This Project is a case study for a start-up, that track the evolution of the stock ratios as well las the financial statements of the stock companies.
- The aim of this project is to create an ETL procces, on the stock data, to answer some different questions that an investing company could have in order to take decisions more accurated.
- The data and metadata about the stoks comes from a Stock web page that conatins information realted with the stock market (news, stock prices, financial statements, and a lot more) called [Financial Modeling Prep] that has an Rest API with a lot of stock information.

## Architecture

The technical architecture for this project is as show below:


![Architecture](https://github.com/Justmaister/Data-Enginering-Stock-Project/blob/master/images/Finance%20Project%20Architecture.png)

1. The Data extraction is done using the Rest API to the Financial Modeling Prep using the GET request.
```sh
https://financialmodelingprep.com/api/v3/income-statement/AAPL?&apikey=demo'
```
2. Copy the data downloaded from the API call to an S3 bucket (Staging)
3. Run the ETL pipeline, proces the Data and ingest it to a Redshift Cluster (Data Warehouse) for analytical purposes.

## Choice of Technologies
- For orchestrating the steps in the pipeline, **Airflow** is chosen as it allows building of data pipelines that are straghtforward and modular. Airflow allows tasks to be defined in a Directed Acyclic Graph (DAG) with dependencies of tasks between one another. This allows running of tasks to be optimized. It also enables the pipeline to be run on a schedule (for eg, daily) should the need arise. Finally, it has an intuitive UI that allows users to check the steps in the data pipeline should any part of the pipeline fail.
- To store all the data in raw format **S3** is chossen to acomplish that task as it is the Data Lake of AWS the cloud infraestructure used in this project.
- **Redshift** is chosen as the cloud Data Warehouse as it is highly scalable. Should our data grow in size, we can provision more nodes or scale up, to handle the larger volume of data.

## Data Model

![Data Model](https://github.com/Justmaister/Data-Enginering-Stock-Project/blob/master/images/Data%20Model.png)

## ETL Pipeline

The ETL process runs through an Airflow DAG:

```sh
Airflow Diagram UI
```

The process is as follows:

We create the tables and staging tables (if they do not exist)
We perform an update and insert, based on new data coming in
Run a data quality check (check that tables have more than 1 row and there are no null ids)

## Potential Improvements
