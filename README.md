# Data Engineering Example ETL Pipeline

The goal of this repo is to show a somewhat contrived example of a data engineering ETL pipeline.
Although it is contrived it will also exaggerate in places maybe taking unnecessary steps for the sake
of demonstrating some import aspect of data engineering or technology that wouldn't be covered otherwise.

### The Goal:

We will be scraping and streaming data engineer job postings so we can perform analysis on what companies
look for when hiring data engineers.

## The Steps

 1. Pull data from variety of sources. This needs to be done continuously and when new job postings are
 found they need to be extracted.
 2. Put these job postings into Kafka as an immutable storage buffer for later transformations/destinations.
 3. Use spark to pull data from kafka (maybe use an aggregate or windowing function) and load into S3.
 4. Pull the data from S3 and use a "machine learning" model to augment the data and load into a postgres
 database.
 5. Use sql to transform the data one more time to curate it for a dashboard
 6. Load from the curated table into the dashboard.
 
 
[put pretty diagram here]

### Step 1: Data at the source

### Step 2: Kafka the immutable queue

### Step 3: Spark the swiss army knife ETL tool

### Step 4: Using flat files

### Step 5: You can't run from sql

### Step 6: Where most data ends up eventually (in a visualization)