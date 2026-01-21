from pyspark import pipelines as dp

@dp.table
def trip_data_streaming():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/transport/trains/trip_data")
    )
