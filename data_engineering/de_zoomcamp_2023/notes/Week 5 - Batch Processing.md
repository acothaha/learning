# WEEK 5: BATCH PROCESSING

### Table of Contents

# **5.1 Introduction to Batch Processing**

## Batch vs Streaming

There are 2 ways of processing data:
- ***Batch Processing***: Processing *chunks* of data at *regular interval*.
    - e.g. Processing taxi trips each month.

      <img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark3.png"  width="" height="">


- ***Streaming***: Processing data *on the fly*.
    - e.g. Processing a taxi trip as soon as it's generated.

      <img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark2.png"  width="" height="">

## Type of batch jobs

A ***batch job*** is a ***job*** (a unit of work) that will process data in batches.

Batch jobs may be *scheduled* in many ways:

- Weekly
- Daily (Very common)
- Hourly (Very common)
- X times per hours
- Every 5 minutes
- etc.

Batch jobs may also be carried out using different technologies:

- Python scripts
    - Python scripts can be run anywhere (Kubernetes, AWS batch, ...)
- SQL (dbt)
- Spark
- Flink
- etc.

## Orchestration batch jobs

Batch jobs are commonly orchestrated with tools such as airflow/prefect

A common workflow workflow for batch jobs may be as follows:


<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark4.png"  width="" height="">


## Pros and cons of batch jobs


- Advantages:
    - Easy to manage. There are multiple tools to manage them.
    - Re-executable. Jobs can be easily retried if they fail.
    - Scalable. Scripts can be executed in more capable machines; Spark can be run in bigger clusters, etc.
- Disadvantages:
    - Delay. Each task of the workflow in the previous section may take a few minutes; assuming the whole workflow takes 20 minutes, we would need to wait those 20 minutes until the data is ready for work.

However, the advantages of batch jobs often compensate for its shortcomings (flaws), and as a result most companies that deal with data tend to work with batch jobs most of the time (~90%).

# **5.2 Introduction to Spark**
## What is Spark?


[Apache Spark](https://spark.apache.org/) is an open-source ***multi-language*** unified analytics **engine** for large-scale data processing.

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark5.png"  width="" height="">

Spark is a distributed data processing **engine** with its components working collaboratively on a cluster of machines. At a high level in the spark architecture, a Spark application consists of a *driver* program that is responsible for orchestrating parallel operations on the ***Spark cluster***. Ther *driver* accesses the distributed components in the cluster—the **spark executors** and **cluster manager**—through a ***SparkSession***

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark7.png"  width="" height="">

It provides high-level APIs in Java, Scala, Python ([PySpark](https://spark.apache.org/docs/latest/api/python/)), R and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including:

- [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) for SQL and structured data processing,
- [pandas API](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_ps.html) on Spark for pandas workloads,
- [MLlib](https://spark.apache.org/docs/latest/ml-guide.html) for machine learning,
- [GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html) for graph processing,
- [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) for incremental computation and stream processing

See [Spark overview](https://spark.apache.org/docs/latest/index.html) for more.

## Why do we need Spark?

Spark is used for transforming data in Data Lake.

There are tools such as Hive, Presto or Athena (an AWS managed Presto) that allow us to express jobs as SQL queries. However, there are times where we need to apply more complex manipulation which are very difficult or even impossible to express with SQL (such as ML models); in those instances, Spark is the tool to use.

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark1.png"  width="" height="">

A typical workflow may combine both tools. Here's an example of a workflow involving Machine Learning:

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark6.png"  width="" height="">

In this scenario, most of the preprocessing would be happening in Athena, so for everything that can be expressed with SQL, it's always a good idea to do so, but for everything else, there's Spark.

# **5.3 Installing Spark**

Follow these instructions to install Spark.

- [Windows](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/windows.md)
- [Linux](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/linux.md)
- [MacOS](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/macos.md)

and follow [this](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/pyspark.md) to run PySpark in Jupyter

## Installation on Linux

**1. Install Java**

Download OpenJDK 11 or Oracle JDK 11. It’s important that the version is 11 because Spark requires 8 or 11.

Here, we will use OpenJDK. This [page](https://jdk.java.net/archive/) is an archive of previously released builds of the OpenJDK.

To install Java, run the following commands.

```bash
# Create directory.
> mkdir spark
> cd spark

# Download and unpack OpenJDK.
> wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
> tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
> ls
jdk-11.0.2
> pwd
/home/aco/spark

# Setup Java.
> export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
> export PATH="${JAVA_HOME}/bin:${PATH}"
> java --version
openjdk 11.0.2 2019-01-15
OpenJDK Runtime Environment 18.9 (build 11.0.2+9)
OpenJDK 64-Bit Server VM 18.9 (build 11.0.2+9, mixed mode)

# Remove the archive.
> rm openjdk-11.0.2_linux-x64_bin.tar.gz
```
**2. Install Spark**

Go to this [page](https://spark.apache.org/downloads.html) to download Apache Spark.

We will use **Spark 3.3.2 (Feb 17 2023)** version and package type **Pre-built for Apache Hadoop 3.3 and later**.

To install Spark, run the following commands:

```bash
# Download and unpack Spark 3.3.1.
> wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
> tar xzfv spark-3.3.2-bin-hadoop3.tgz

# Setup Spark.
> export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
> export PATH="${SPARK_HOME}/bin:${PATH}"

# Remove the archive.
> rm spark-3.3.2-bin-hadoop3.tgz
```

We should see this

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark8.png"  width="" height="">

To close Spark Shell, you press `Ctrl+D` or type in `:quit` or `:q`

**3. Add PATH to `.bashrc` file**

Add these lines to the bottom of the `.bashrc` file. Use `nano .bashrc`

```bash
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```

Press `Ctrl+O` to save the file and `Ctrl+X` to exit.

Then run the following commands

```bash
> source .bashrc

# Quit the server.
> logout

# Connect to Ubuntu server.
> ssh de-zoomcamp
> which java
/home/boisalai/spark/jdk-11.0.2/bin/java
> which pyspark
/home/boisalai/spark/spark-3.3.2-bin-hadoop3/bin/pyspark
```

**4. Using PySpark**

To run PySpark, we first need to add it to `PYTHONPATH`.

`PYTHONPATH` is a special environment variable that provides guidance to the Python interpreter about where to find various libraries and applications. See [Understanding the Python Path Environment Variable in Python](https://www.simplilearn.com/tutorials/python-tutorial/python-path) for more information.

Starting with adding these instructions to the bottom of cloud VM `~/.bashrc` file with `nano ~/.bashrc`

```bash
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

Make sure that the version under `$SPARK_HOME/python/lib/` matches the filename of `py4j` or you will encounter `ModuleNotFoundError: No module named 'py4j'` while executing `import pyspark`

Press `Ctrl+O` to save the file and `Ctrl+X` to exit.

Then, run this command: `source ~/.bashrc.`

# **5.4 First look at Spark/PySpark**

## Creating a Spark Session

WE can use Spark with Python code by means of PySpark. We will be using Jupyter Notebooks for this session.

We first need to import PySpark to our code:

```python
import pyspark
from pyspark.sql import SparkSession
```

Now, we need to intantiate a ***Spark Session***, an object that we use to interact with Spark.

```python
spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
```

- `SparkSession` is the class of the object that we instantiate. `builder` is the builder method.
- `master()` setes the Spark *master URL* to connect to. The `local` string means that Spark will run on a local cluster. `[*]` means that Spark will run with as many CPU cores as possible.
- `appName` defines the name of the application/session. This will be shown in the Spark UI.
- `getOrCreate` will create the session or recover the object if it was previously created.

Once we've instantiated a session, we can access the Spark UI by browsing to `localhost:4040`. The UI will display all current jobs. Since we've just created the instance, there should be no jobs currently running.

## Reading CSV files

Similarly to Pandas, Spark can read CSV files into ***dataframes***, a tabular data structure. Unlike Pandas, Spark can handle much bigger datasets but it's unable to infer the datatypes of each columns.

> Note: Spark dataframes use custom data types; we cannot use regular Python data types.

For this example, we will use the [High Volume For-Hire Vehicle Trip Records for January 2021](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz) available from the [NYC TLC Trip Record Data website](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhvhv). The file should be about 124MB in size.

To download the file run this commands

```bash
# Download the file
> wget "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz"

# unzip the file
> gzip -d "fhvhv_tripdata_2021-01.csv.gz"

# Check number of lines in the CSV
> wc -l "fhvhv_tripdata_2021-01.csv"
11908469 fhvhv_tripdata_2021-01.csv
```


Let's read the file and create a dataframe:

```python
df = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-01.csv')
```

- `read()` reads the file.
- `option()` contains options for the `read()` method. In this case, we're specifying that the first line of the CSV file contains the columns names.
- `csv()` is for reading CSV files.

you can see the contents of the dataframe with `df.show()` or `df.head()`. You can also check the current schema with `df.schema()` or `df.printSchema()`; you will notice that all values are strings.

We can use a trick with Pandas to infer the datatypes:
1. Create a smaller CSV file with the first 1000 records or so.
2. Import Pandas and create a Pandas dataframe. This dataframe will have inferred datatypes.
3. Create a Spark dataframe from the Pandas dataframe and check its schema.

    ```python
    spark.createDataFrame(my_pandas_datafram).schema
    ```

4. Based on the output of the previous method, import `types` from `pyspark.sql` and create `StructType` containing a list of the datatypes.

    ```python
    from pyspark.sql import types
    schema = types.Structype([...])
    ```
    - `types` contains all of the available data types for Spark dataframes.

5. Create a new Spark dataframe and include the schema as an option.
    ```python
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv('fhvhv_tripdata_2021-01.csv')
    ```

You may find an example of Jupyter Notebook file using this trick [here](https://github.com/acothaha/learning/blob/main/data_engineering/de_zoomcamp_2023/week_5_batch_processing/notebooks/04_pyspark.ipynb)

## Partitions

A ***Spark cluster*** is composed of multiple ***executors***. Each executor can process data independently in order to parallelize and speed up work.

In the previous example we read a single large CSV file. A file can only be read by a single executor, which means that the code we've written so far isn't parallelized and thus will only be run by a single executor rather than many at the same time.

In order to solve this issue, we can *split a file into multiple parts* so that each executor can take care of the part and have all executors working simultaneously. These splits are called partitions.

We will now read the CSV file, partition the dataframe and parquetize it. This will create multiple files in parquet format.

> converting to parquet is an **expensive** operation which may take several minutes.

```python
# Create 24 partitions in our dataframe
df = df.repartition(24)
# parquetize and write to fhvhv/2021/01/ folder
df.write.parquet('fhvhv/2021/01/')
```

We may check the Spark UI at any time and see the progress of the current job, which is divided into stage which contains tasks. The tasks in a stage will not start until all task on the previous stage are finished.

When creating a dataframe, Spark creates as many partitions as CPU cores available by default, and each partition creates a task. Hence, assuming that the dataframe was initially partitioned into 6 partitions, the `write.parquet()` method will have 2 stages: the first with 6 tasks and the second one with 24 tasks.

Besides the 24 parquet files, you should also see a `_SUCCESS` file which should be empty. This file is created when the job is finished successfully.

Trying to write the files again will output an error because Spark will not write to a non-empty folder. You can force an overwrite with the `mode` argument:

```python
df.write.parquet('fhvhv/2021/01/', mode='overwrite')
```

The opposite of partitioning (joining multiple partitions into a single partition) is called ***coalescing***.

## Spark dataframes

We can create a dataframe from the parquet files we create in the previous section.

```python
df = spark.read.parquet('fhvhv/2021/01/')
```

Unlike CSV files, parquet files contain the schema of the dataset, so there is no need to specify a schema like we previously did when reading the CSV files. you can check the schema like this:

```python
df.printschema()
```

(One of the reasons why parquet files are relatively smaller than CSV files is because they store the data according to the datatypes, so integer values will take less space than long or string values)

There are many Pandas-like operations that we can do on spark dataframe, such as:

- Column selection - returns a dataframe with only the selected columns

    ```python
    new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')
    ```

 - Filtering by values - return a dataframe whose records match the condition stated in the filter

    ```python
    new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID').filter(df['hvfhs_license_num'] == 'HV0003')

- And many more. The official Spark documentation website contains a quick guide for dataframes ([here](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html))

## Actions vs Transformations

Some Spark methods are "**Lazy**", meaning that they are not executed right away. We can test this with the last instructions we run in the previous section: after running them, the Spark UI will not show any new jobs. On the other hand, running `df.show()` will be executed right away and display the contents of the dataframe; the Spark UI will also show a new job.

These lazy commands are called ***transformations*** and the eager commands are called ***actions***. Computations only happen when actions are triggered.

```python
df.select(...).filter(...).show()
```
<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark9.png"  width="" height="">

Both `select()` and `filter()` are ***transformations***, but `show()` is an ***action***. The whole instructions gets evaluated only when the `show()` action is triggered.

list of ***transformations*** (lazy):

- Selecting columns
- Filtering
- Joins
- Group by
- Partitiions
- ...

List of ***actions*** (eager):
- Show, take, head
- Write, read
- ...

## Functions and User Defined Functins (UDFs)
 
Besides the SQL and Pandas-like commands we've discussed so far, Spark provides additional built-in functions that allow for more complex data manipulation. By convention, these functions are imported as follows:

```python
from pyspark.sql import functions as F 
```

Here is an example of built-in function usage:

```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```
- `withColumn()` is a ***transformation*** that adds a new column to the dataframe.

    > ***IMPORTANT***: adding a new column with the same name as a previously existing column will overwrite the existing column!
-  `select()` is another ***transformation*** that select the stated columns.
- `F.to_date()` is a built-in Spark function that converts a timestamp to date format (year, month and day only, no hour and minute)

A list of built-in functions is available [here](https://spark.apache.org/docs/latest/api/sql/index.html)

Besides these built-in functions, Spark allows us to create ***User Defined Functions*** (UDFs) with custom behaviour for those instances where creating SQL queries for that behaviour becomes difficult both to manage and test.

UDFs are regular functions which are then passed as parameters to a special builder. Let's create one:

```python
# A crazy function that changes values when they're divisible by 7 or 3
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'

# Creating the actual UDF
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
```

- `F.udf()` takes a function (`crazy_stuff()` in this example) as parameter as well as a return type for the function (a string in our example).
- While `crazy_stuff()` is obviuosly non-sensical, UDFs are handy for things such as ML and other complex operations for which SQL isn't suitable or desirable. Python code is also easier to test than SQL.

We can then use our UDF in ***transformations*** just like built-in functions:

```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

# **5.5 Spark SQL**

We already mentioned at the beginning that there are other tools for expressing batch jobs as SQL queries. However, Spark can also run SQL queires, which can come in handy if you already have Spark cluster and setting up an additional tool for sporadic use isn't desireable.

## Combining the 2 datasets

> ***NOTE***: this block makes use of the yellow and green taxi datasets for 2020 and 2021 as parquetized local files. It can be downloaded and parquetized the files directly; [check out this extran lesson](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/extra1_preparing_data.md) (from ziritrion) to see how

Let's now load all of the yellow and green taxi data for 2020 and 2021 to Spark dataframes.

Assuming the parquet files for the dataset are stored on a `data/pq/color/year/month` folder structure:

```python
df_green = spark.read.parquet('data/pq/green/*/*')
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet('data/pq/yellow/*/*')
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
```
- Because the pickup and dropoff column names don't match between 2 datasets, we use the `withColumnRenamed`action to make them have matching name.

We will replicate the [`dim_monthly_zone_revenue.sql`](https://github.com/acothaha/dbt_ny_taxi_rides_zoomcamp/blob/main/models/core/dim_monthly_zone_revenue.sql) model from lesson 4 in Spark. This model makes use of `trips_data`, a combined table of yellow and green taxis, so we will create a combined dataframe with the commong columns to both datasets.

We need to find our which are the common columns. We could do this:

```python
set(df_green.columns) & set(df_yellow.columns)
```

However, this command will not respect the column order. We can do this instead to respect the order:

```python
common_colums = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_colums.append(col)
```

Before we combine the datasets, we need to figure out how we will keep track of the taxi type for each record (the `service_type` field in `dim_monthly_zone_revenue.sql`). We will ad the service_type column to each dataframe.

```python
from pyspark.sql import functions as F

df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))
```
- `F.lit` adds a *literal* or constant to a dataframe. We use it here to fill the `service_type` column with a constant value, which is its corresponding taxi type.

Finally, let's combine both datasets:
```python
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
```
We can also count the amount of records per taxi type:
```python
df_trips_data.groupBy(`service_type`).count.show()
```

## Querying a dataset with Temporary Tables

We can make SQL queries with Spark with `spark.sqll("SELECT * FROM ???")`. SQL expects a table for retrieving records, but a dataframe is not a table, so we need to ***register*** the dataframe as a table first:

```python
df_trips_data.createOrReplaceTempView('trips_data')
```
- This method creates a ***temporary table*** with the name `trips_data`

With our registered table, we can now perform regular SQL operations:

```python
spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()
```

- This query outputs the same as `df_trips_data.groupBy('service_type').count().show()`
- Note that the SQL query is wrapped with 3 double quotes (`"`)

The query output can be manipulated as a dataframe, which means that we can perform any queries on our table and manipulate the results with Python as we see fit.

We can now slightly modify the [`dim_monthly_zone_revenue.sql`](https://github.com/acothaha/dbt_ny_taxi_rides_zoomcamp/blob/main/models/core/dim_monthly_zone_revenue.sql), and run it as a query with Spark and store the output in a dataframe:

```python
df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")
```

- We removed the `with` statement from the original query because it operates on an external table that Spark does not have access to.
- We removed the `count(tripid) as total_monthly_trips`, line in *additional calculations* because in alse depends on that external table.
- We change the grouping from field names to references in order to avoid mistake.

SQL queries are ***transformations***, so we need an action to perform them such as `df_result.show()`.

once we're happy with the output, we can also store it as a parquet file just like any other dataframe. We could run this:

```python
df_result.write.parquet('data/report/revenue/')
```

However, with our current dataset, this will create more that 200 parquet files of very small size, which isn't very desirable.

In order to reducre the amount of files, we need to reduce the amount of partitions of the dataset, which is done with the `coalesce()` method:

```python
df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')
```

This reduces the amount of partitions to just 1

# **5.6 Spark internals**

## Spark Cluster

Until now, we've used a ***local cluster*** to run our Spark code, but Spark cluster often contain multiple computers that behace(act) as executors.

Spark clusters are managed by a ***master***, which behaves similarly to an entry point of a kubernetes cluster. A ***driver*** (an Airflow DAG, a computer running a local script, etc) that wants to execute a Spark job will send the job to the master, which in turn will divide the work among the cluster's executors. If any executor fails and becomes offline for any reason, the master will reassign the task to another executor.

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark10.png"  width="" height="">

Each executo will fetch a ***dataframe partition*** stored in a ***Data Lake*** (usually S3, GCS or similar cloud provider), do something with it and then store it somewhere, which could be the same Data Lake or somewhere else. If there are more partitions that executors, executors will keep fetching partitions until every single one has been processed.

This is in contrast to [Hadoop](https://hadoop.apache.org/), another data analytics engine, whose executors locally store the data they process. Partitions in Hadoop are duplicated across several executors for redudancy, in case an executor fails for whatever reason (Hadoop is meant for clusters made of commodity hardware computers). However, data locality has become less important as storage and data transfer costs have dramatically decreased and nowadays it's feasible to seperate storage from computation, so Hadoop has fallen out of fashion.

## GROUP BY in Spark

Let's do the following query:

```python
df_green_revenue = spark.sql("""
SELECT 
    date_trunc('hour', lpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2  
""")
```

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark11.png"  width="" height="">