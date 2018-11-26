MIGRATE_CASSANDRA_DATA_TO_HADOOP = \
    "import datetime\n" \
    "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType\n\n" \
    "rdd = sc.parallelize({rdd_input}, 1)\n" \
    "{schema_text}\n" \
    "df = spark.createDataFrame(rdd, schema=schema)\n" \
    "df.write.orc('{tweet_data_hdfs_path}', mode='append')"
