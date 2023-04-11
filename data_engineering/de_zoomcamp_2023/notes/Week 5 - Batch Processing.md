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

Spark is an **engine** because it *processes* data.

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark5.png"  width="" height="">

Spark can be ran in *clusters* with multiple *nodes*, each pulling and transforming.

Spark is a ***multi-language*** because we can use Java and Scala natively, and there are wrappers for Python, R and other languages.

The wrapper for Python is called [PySpark](https://spark.apache.org/docs/latest/api/python/)

Spark can deal with both batches and streaming data. The technique for streaming data is seeing a stream of data as a sequence of small batches and then applying the similar techniques on them to those used on regular batches. Streaming will be covered in detail in the next session.

## Why do we need Spark?

Spark is used for transforming data in Data Lake.

There are tools such as Hive, Presto or Athena (an AWS managed Presto) that allow us to express jobs as SQL queries. However, there are times where we need to apply more complex manipulation which are very difficult or even impossible to express with SQL (such as ML models); in those instances, Spark is the tool to use.

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark1.png"  width="" height="">

A typical workflow may combine both tools. Here's an example of a workflow involving Machine Learning:

<img style="margin: 2em; display: block; margin-left: auto; margin-right: auto;" src="images/spark6.png"  width="" height="">

In this scenario, most of the preprocessing would be happening in Athena, so for everything that can be expressed with SQL, it's always a good idea to do so, but for everything else, there's Spark.