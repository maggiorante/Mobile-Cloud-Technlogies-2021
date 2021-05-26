###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, struct

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
tedx_dataset_path = "s3://miobucketunibg1/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline","true").csv(tedx_dataset_path)
    
tedx_dataset.printSchema()

#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset_path = "s3://miobucketunibg1/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

tedx_dataset_agg.printSchema()

## READ SUBTITLES DATASET
subtitles_dataset_path = "s3://miobucketunibg1/TED_Talk.csv"
subtitles_dataset = spark.read.option("header","true").csv(subtitles_dataset_path)

# CREATE THE AGGREGATE MODEL, ADD SUBTITLES TO TEDX_DATASET
subtitles_dataset_agg = subtitles_dataset.groupBy(col("url__webpage").alias("transcript_url")).agg(collect_list("transcript").alias("subtitles"))
subtitles_dataset_agg= subtitles_dataset_agg.where(col("subtitles").isNotNull())
subtitles_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset_agg.join(subtitles_dataset_agg, tedx_dataset_agg.url == subtitles_dataset_agg.transcript_url, "left") \
    .drop("transcript_url") \
    #.select(col("idx").alias("_id"), col("*")) \
    #.drop("idx") \

subtitles_dataset_agg.printSchema()

## READ WATCHNEXT DATASET
watchnext_dataset_path = "s3://miobucketunibg1/watch_next_dataset.csv"
watchnext_dataset = spark.read.option("header","true").csv(watchnext_dataset_path)
watchnext_dataset=watchnext_dataset.dropDuplicates().where('url!="https://www.ted.com/session/new?context=ted.www%2Fwatch-later"')

watchnext_dataset=watchnext_dataset.select(col("idx").alias("id"), col("*")).drop("idx")
watchnext_dataset=watchnext_dataset.select(col("url").alias("link"), col("*")).drop("url")

watchnext_dataset=watchnext_dataset.join(tedx_dataset,tedx_dataset.idx==watchnext_dataset.watch_next_idx)

# CREATE THE AGGREGATE MODEL, ADD WATCHNEXT TO TEDX_DATASET
watchnext_dataset_agg=watchnext_dataset.groupBy(col("id").alias("idx_ref")).agg(collect_list(struct('link','watch_next_idx','main_speaker','title','posted')).alias("watch_next"))
watchnext_dataset_agg.printSchema()
watchnext_dataset_agg = tedx_dataset_agg.join(watchnext_dataset_agg, tedx_dataset_agg._id == watchnext_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    #.select(col("idx").alias("_id"), col("*")) \
    #.drop("idx") \

watchnext_dataset_agg.printSchema()

mongo_uri = "mongodb://cluster0-shard-00-00.jrgnj.mongodb.net:27017,cluster0-shard-00-01.jrgnj.mongodb.net:27017,cluster0-shard-00-02.jrgnj.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "triviated",
    "username": "admin123",
    "password": "admin123",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(watchnext_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
