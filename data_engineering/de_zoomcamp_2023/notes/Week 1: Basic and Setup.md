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
```powershell
python pipeline.py <day>
```

It will print 2 lines:

```
['pipeline.py', '<day>']

job finished successfully for day = <day>
```
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

```Powershell 
docker build -t tests:pandas .
```

- The image name will be `tests` and its tag will be `pandas`. if the tag isn't specified it will default `latest`.

We can check the our previous image in the docker by passing:

```Powershell 
docker images -ls
```

Since the image is there, now we can run the container and pass an argument to it, so that our pipeline will receive it:

```Powershell 
docker run -it test:pandas some_number
```

you will get the same output you did when you ran the pipeline script itself.

>Note: these instructions assume that `pipeline.py` and `Dockerfile` are in the same directory. The Docker commands should also be run from the same directory as these files.


### **Running Postgres in a container**

You can run a containerized version of Postgres that doesn't require any installation steps. You only need to provide a few *environment* variables to it as well as a *volume* for storing data.

Create a foler anywhere you'd like for Postgres to store data in. We will use the example folder `ny_taxi_postgres_data`. Here's how to run the container:

```POWERSHELL
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
   postgres:13
```


- The container needs 3 environment variables:

    - `POSTGRES_USER` is the username for logging into the database. We chose `root`.
    - `POSTGRES_PASSWORD` is the password for the database. We chose `root`

        > ***IMPORTANT: These values are only meant for testing. Please change them for any serious project.***
    - `POSTGRES_DB` is the name that we will give the database. We chose `ny_taxi`.

- `-v` points to the volume directory. The colon `:` separates the first part (path to the folder in the host computer) from the second part (path to the folder inside the container).
    - Path names must be absolute. If you're in a UNIX-like system, you can use `pwd` to print you local folder as a shortcut; this example should work with both `bash` and `zsh` shells
    - This command will only work if you run it from a directory which contains the `ny_taxi_postgres_data` subdirectory you created above.
- `-p` is for port mapping. We map the default Postgres port to the same port in the host (5432).
- The last argument is the image name and tag. We run the official postgres image on its version `13`.

> Note: `-it` is short for --interactive. When you docker run with this command, it takes you straight inside the container.

[Once the container is runing, we can log into our database with [pgcli](https://www.pgcli.com/) with the following command:

```POWERSHELL
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

- `-h` is the host. Since we're running locally we can use `localhost`.
- `-p` is the port.
- `-u` is the username.
- `-d` is the database name.
- The password is not provided; it will be requested after running the command.

### **Ingesting data to Postgres with Python**d

We will now use Jupyter Notebook to read a SCV file and export it into Postgres.

We will use data from the [NYC TLC Trip Record Data website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Specifically, we will use the [Yellow taxi trip records CSV file for January 2021](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow). A dictionary to understand each field is available [here](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

> Note: knowledge of Jupyter Notebook, Python environment management and Pandas is assumed in these notes. Please check [this link](https://gist.github.com/ziritrion/9b80e47956adc0f20ecce209d494cd0a#pandas) for a Pandas cheatsheet and [this link](https://gist.github.com/ziritrion/8024025672ea92b8bdeb320d6015aa0d) for a Conda cheatsheet for Python environment management.

Check the completed `upload-data.ipynb` [here](https://github.com/acothaha/learning/blob/main/data_engineering/de_zoomcamp_2023/week_1_basics_n_setup/2_docker_sql/upload_data.ipynb) for a detailed guide. Feel free to copy the file to your work directory; in the same directory you will need to have the CSV file linked above and the ny_taxi_postgres_data subdirectory.