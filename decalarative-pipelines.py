from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType  # Adjust types as needed
from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql import functions as F

data_schema = StructType([
    StructField("Year", StringType(), True),
    StructField("Month",StringType(), True),
    StructField("Departure_station", StringType(), True),
    StructField("Arrival_station", StringType(), True),
    StructField("Average_travel_time_min", DoubleType(), True),
    StructField("Number_of_expected_circulations", DoubleType(), True),
    StructField("Number_of_cancelled_trains", DoubleType(), True),
    StructField("Number_of_late_trains_at_departure", DoubleType(), True),
    StructField("Average_delay_of_late_departing_trains_min", DoubleType(), True),
    StructField("Average_delay_of_all_departing_trains_min", DoubleType(), True),
    StructField("Comment_dep_delays", StringType(),True),
    StructField("Number_of_trains_late_on_arrival",DoubleType(), True),
    StructField("Avg_Delay_of_late_Arriving_trains", DoubleType(), True),
    StructField("Avg_Delay_of_all_Arriving_trains", DoubleType(), True),
    StructField("Comment_arrival_delays", StringType(),True),
    StructField("Per_trains_late_due_to_external_causes", DoubleType(), True),
    StructField("Per_trains_late_due_to_railway_infrastructure", DoubleType(), True),
    StructField("Per_trains_late_due_to_traffic_management", DoubleType(), True),
    StructField("Per_trains_late_due_to_rolling_stock", DoubleType(), True),
    StructField("Per_trains_late_due_to_station_management", DoubleType(), True),
    StructField("Per_trains_late_due_to_passenger_traffic", DoubleType(), True),
    StructField("Number_of_late_trains_greater_than_15_min", DoubleType(), True),
    StructField("Average_train_delay_greater_than_15_min", DoubleType(), True),
    StructField("Number_of_late_trains_greater_than_30_min", DoubleType(), True),
    StructField("Number_of_late_trains_greater_than_60_min", DoubleType(), True),
    StructField("Period", StringType(), True),
    StructField("Delay_due_to_external_causes", DoubleType(), True),
    StructField("Delay_due_to_railway_infrastructure", DoubleType(), True),
    StructField("Delay_due_to_traffic_management", DoubleType(), True),
    StructField("Delay_due_to_rolling_stock", DoubleType(), True),
    StructField("Delay_due_to_station_management_and_reuse_of_material", DoubleType(), True),
    StructField("Delay_due_to_passengers",DoubleType(), True)
])

@dp.table
def delay_data_streaming():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .schema(data_schema)
        .load("/Volumes/transport/trains/train_departures/")
    )

@dp.table(
    schema="""
        Year STRING,
        month INT,
        Departure_station STRING,
        Arrival_station STRING,
        primary_key STRING,
        Average_travel_time_min DOUBLE,
        Number_of_expected_circulations DOUBLE,
        Number_of_cancelled_trains DOUBLE,
        Number_of_late_trains_at_departure DOUBLE,
        Average_delay_of_late_departing_trains_min DOUBLE,
        Average_delay_of_all_departing_trains_min DOUBLE,
        Number_of_trains_late_on_arrival DOUBLE,
        Avg_Delay_of_late_Arriving_trains DOUBLE,
        Avg_Delay_of_all_Arriving_trains DOUBLE,
        Number_of_late_trains_greater_than_15_min DOUBLE,
        Average_train_delay_greater_than_15_min DOUBLE,
        Number_of_late_trains_greater_than_30_min DOUBLE,
        Number_of_late_trains_greater_than_60_min DOUBLE,
        Period STRING,
        Delay_due_to_external_causes DOUBLE,
        Delay_due_to_railway_infrastructure DOUBLE,
        Delay_due_to_traffic_management DOUBLE,
        Delay_due_to_rolling_stock DOUBLE,
        Delay_due_to_station_management_and_reuse_of_material DOUBLE,
        CONSTRAINT silver_delay_data_pk PRIMARY KEY (primary_key)
    """
)
def silver_delay_data():
    return (
        dp.read("delay_data_streaming")
        .select(
            "Year",
            F.col("month").cast("INT").alias("month"),
            "Departure_station",
            "Arrival_station",
            F.concat_ws(
                "_",
                F.col("Year"),
                F.col("month"),
                F.col("Departure_station"),
                F.col("Arrival_station")
            ).alias("primary_key"),
            "Average_travel_time_min",
            "Number_of_expected_circulations",
            "Number_of_cancelled_trains",
            "Number_of_late_trains_at_departure",
            "Average_delay_of_late_departing_trains_min",
            "Average_delay_of_all_departing_trains_min",
            "Number_of_trains_late_on_arrival",
            "Avg_Delay_of_late_Arriving_trains",
            "Avg_Delay_of_all_Arriving_trains",
            "Number_of_late_trains_greater_than_15_min",
            "Average_train_delay_greater_than_15_min",
            "Number_of_late_trains_greater_than_30_min",
            "Number_of_late_trains_greater_than_60_min",
            "Period",
            "Delay_due_to_external_causes",
            "Delay_due_to_railway_infrastructure",
            "Delay_due_to_traffic_management",
            "Delay_due_to_rolling_stock",
            "Delay_due_to_station_management_and_reuse_of_material"
        )
    )
