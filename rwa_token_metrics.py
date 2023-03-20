# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

raw_logs_df = spark.read.format("parquet").load("gs://rwa-eth-bq-exports/logs")

# COMMAND ----------

abi_contracts = []
addresses = ["0x1a4b46696b2bb4794eb3d4c26f1c55f9170fa4c5", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c"]

for address in addresses:
    abi_contract=get_abi_events(address=address)
    abi_contracts.append({'address':address, 'abi_contract': abi_contract})

abi_contracts_df = spark.createDataFrame(data=abi_contracts)
display(abi_contracts_df)

# COMMAND ----------

raw_logs_with_abi_df = raw_logs_df.alias("raw_logs").join(abi_contracts_df.alias("contracts"), raw_logs_df['address']==abi_contracts_df['address'], "left")\
                                    .filter(col("contracts.abi_contract").isNotNull())\
                                    .select("raw_logs.*", "contracts.abi_contract")

# COMMAND ----------

from pyspark.sql.functions import col, udf, lit, year, month, concat

parsed_df = raw_logs_with_abi_df\
            .withColumn('parsed_log_dict', decode_log(col("address"), col("topics"), col("data"), col("abi_contract")))\
            .withColumn('block_year', year(col("block_timestamp")))\
            .withColumn('block_month', month(col("block_timestamp")))\
            .withColumn('log_from', col("parsed_log_dict.from"))\
            .withColumn('log_to', col("parsed_log_dict.to"))\
            .withColumn('log_value', to_decimal(col("parsed_log_dict.value")))\
            .filter(col("log_value").isNotNull())

# COMMAND ----------

from pyspark.sql.functions import sum,count

total_transfer_value_df = parsed_df.groupBy("address", "block_year", "block_month")\
                                    .agg(\
                                        count("*").alias("count"),\
                                        sum("log_value").alias("total_transfer_value")\
                                        )

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import max

month_year_window = Window.partitionBy("block_year", "block_month")

# most transferred token by transfer value 
most_transferred_tokens_by_value = total_transfer_value_df.withColumn("max_transfer_value", max(col("total_transfer_value")).over(month_year_window))\
                                  .filter(col("total_transfer_value") == col("max_transfer_value"))\
                                  .drop("max_transfer_value")

most_transferred_tokens_by_value.write.format('csv').save("/dbfs/user/rwa/metrics/by_transfer_value")

# most transferred token by number of transfers
most_transferred_tokens_by_count = total_transfer_value_df.withColumn("max_transfer_by_count", max(col("count")).over(month_year_window))\
                                  .filter(col("count") == col("max_transfer_by_count"))\
                                  .drop("max_transfer_by_count")

most_transferred_tokens_by_count.write.format('csv').save("/dbfs/user/rwa/metrics/by_transfer_count")

# COMMAND ----------


