# WEEK 1: BASIC AND SETUP
---

## Table of Contents



[**3.1 Data Warehouse And BigQuery**](#31-data-warehouse-and-bigquery)
* [OLAP vs OLTP](#olap-vs-oltp)
* [Data Warehouse](#data-warehouse)
* [BigQuery](#bigquery)
* [BigQuery Cost](#bigquery-cost)
* [BigQuery Query](#bigquery-query)

[**3.2 Partitioning And Clustering**](#32-partitioning-and-clustering)
* [Partition in BigQuery](#partition-in-bigquery)
* [BigQuery Partition Query](#bigquery-partition-query)
* [Clustering in BigQuery](#clustering-in-bigquery)
* [BigQuery Partition + Clustering Query](#bigquery-partition-+-clustering-query)
* [Partitioning vs Clustering](#partitioning-vs-clustering)

[**3.3 BigQuery Best Practices**](#33-bigquery-best-practices)
* [Cost Reduction](#cost-reduction)
* [Query Performance](#query-performance)




## 1.1 Introduction to Data Engineering

***Data engineering*** is the design and development of systems of collecting, storing and analyzing data at scale.

### **Architecture**

Along the course, we will replicate the following architecture:

<img src="images/w1_arch.png"  width="700" height="500">

- [*NY TLC Data Website*](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): The dataset which will be used in this course
- *Google Cloud Platform (GCP)*: Cloud-based auto-scaling platform by Google
    - [*Google Cloud Storage (GCS)*](https://cloud.google.com/storage): A managed service for storing unstructured data (Data Lake)
    - [*BigQuery*](https://cloud.google.com/bigquery): Serverless and cost-effective enterprise data warehouse
- [*Terraform*](https://www.terraform.io/): An infrastructure-as-Code (IaC) tool that lets you build, change, and version cloud and on-prem resources safely and efficiently
- [*Docker*](https://www.docker.com/): A platform designed to help developers build, share, and run modern applications (Containerization)
- *SQL*: Data Analysis & Exploration
- [*Prefect*](https://www.prefect.io/): Workflow orchestration tool for coordinating all data tools
- [*dbt*](https://docs.getdbt.com/docs/introduction): Command line tool that enables data analysts and engineers to transform data in their warehouses more effectively (Data transformation)
- [*Spark*](https://spark.apache.org/): Analytics engine for large-scale data processing (Distributed Processing). 
- [*Kafka*](https://kafka.apache.org/): Unified, high-throughput,low-latency platform for handling real-time data feeds (Streaming).

### **Data Pipelines**

A data pipeline is a service that receives data as input and outputs more data. For example, reading a CSV file, transforming the data somehow and storing it as a table in a PostgreSQL database.

<img src="images/w1_data_pipelines.png"  width="600" height="300">

## 1.2 Docker and Postgres

### **Docker Basic Concepts**

**Docker** is a *containerization* software that allows us to isolate software in a similar way to virtual machines but in a much leaner way.

A **Docker image** is a snapshot of a container that we can define to run our software, or in this case our data pipelines. By exporting our Docker images to Cloud providers such as Amazon Web Services or Google Cloud Platform we can run our containers there.

Docker provides the following advantages:
- Reproducibility
- Local experimentation
- Integration tests (CI/CD)
- Running pipelines on the cloud (AWS Batch, Kubernetes jobs)
- Spark (for defining data pipelines)
- Serverless (AWS Lambda, Google functions)

Docker containers are ***stateless***: any changes done inside a container will **NOT** be saved when the container is killed and started again. This is an advantage because it allows us to restore any container to its initial state in a reproducible manner, but you will have to store data elsewhere if you need to do so; a common way to do so is with *volumes*.

### **Creating a simple "data pipeline" in Docker**

We will create a simple "data pipeline" using python `pipeline.py` that receinves an argument and print in.

```python

import sys
import pandas as pd # we don't need this but it's useful for the example

# print arguments
print(sys.argv)

# argument 0 is the name os the file
# argumment 1 contains the actual first argument we care about
day = sys.argv[1]

# print a sentence with the argument
print(f'job finished successfully for day = {day}')
```

We can run this script in CLI 

> `python pipeline.py <day>`


It will print 2 lines:

> `['pipeline.py', '<day>']`

> j`ob finished successfully for day = <day>`

Let's containerize it by creating a Docker image. Create the following `Dockerfile`file:

```Dockerfile
# base Docker image that we will build on
FROM python:3.9.1

# set up our image by installing prerequisites; pandas in this case
RUN pip install pandas

# set up the working directory inside the container
WORKDIR /app
# copy the script to the container. 1st name is source file, 2nd is destination
COPY pipeline.py pipeline.py

# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT ["python", "pipeline.py"]
```

Let's build the image:

> docker build -t test:pandas .



