#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum as _sum, upper, lit, dense_rank
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder.appName("Task1.1").getOrCreate()
    athletes_2012 = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
    athletes_2016 = spark.read.csv(sys.argv[2], header=True, inferSchema=True)
    athletes_2020 = spark.read.csv(sys.argv[3], header=True, inferSchema=True)
    coaches_df = spark.read.csv(sys.argv[4], header=True, inferSchema=True)
    medals_df = spark.read.csv(sys.argv[5], header=True, inferSchema=True)

    athletes_2012 = athletes_2012.withColumn("year", lit(2012))
    athletes_2016 = athletes_2016.withColumn("year", lit(2016))
    athletes_2020 = athletes_2020.withColumn("year", lit(2020))

    athletes_df = athletes_2012.union(athletes_2016).union(athletes_2020).dropDuplicates()
    athletes_df = athletes_df.withColumn("name", upper(col("name")))
    athletes_df1 = athletes_df.select(*[col(c).alias(c.lower()) for c in athletes_df.columns]) \
                             .withColumn("name", upper(col("name"))) \
                             .withColumn("country", upper(col("country"))) \
                             .filter(col("country").isin("INDIA", "CHINA", "USA"))

    coaches_df = coaches_df.select(
        col("id").alias("coach_id"),
        upper(col("name")).alias("coach_name"),
        col("age").alias("coach_age"),
        col("years_of_experience").alias("coach_years_of_experience"),
        upper(col("sport")).alias("coach_sport")
    ).dropDuplicates()

    medals_df = medals_df.select(
        col("id").alias("athlete_id"),
        upper(col("sport")).alias("medal_sport"),
        upper(col("event")).alias("medal_event"),
        col("year").alias("medal_year"),
        upper(col("country")).alias("medal_country"),
        upper(col("medal")).alias("medal_type")
    ).dropDuplicates()

    medals_with_points = medals_df.withColumn(
        "points",
        when((col("medal_year") == 2012) & (col("medal_type") == "GOLD"), 20)
        .when((col("medal_year") == 2012) & (col("medal_type") == "SILVER"), 15)
        .when((col("medal_year") == 2012) & (col("medal_type") == "BRONZE"), 10)
        .when((col("medal_year") == 2016) & (col("medal_type") == "GOLD"), 12)
        .when((col("medal_year") == 2016) & (col("medal_type") == "SILVER"), 8)
        .when((col("medal_year") == 2016) & (col("medal_type") == "BRONZE"), 6)
        .when((col("medal_year") == 2020) & (col("medal_type") == "GOLD"), 15)
        .when((col("medal_year") == 2020) & (col("medal_type") == "SILVER"), 12)
        .when((col("medal_year") == 2020) & (col("medal_type") == "BRONZE"), 7)
        .otherwise(0)
    )
    medals_with_points = medals_with_points.withColumn("points", col("points").cast("int"))

    athletes_with_points = medals_with_points.join(
        athletes_df1, 
        (medals_with_points.athlete_id == athletes_df1.id) & 
        (medals_with_points.medal_year == athletes_df1.year) &
        (medals_with_points.medal_event == upper(athletes_df1.event)), 
        "inner"
    ).dropDuplicates()

    coaches_with_points = athletes_with_points.join(
        coaches_df, 
        (athletes_with_points.coach_id == coaches_df.coach_id) & 
        (upper(athletes_with_points.sport) == coaches_df.coach_sport),  
        "inner"
    ).dropDuplicates()

    coach_points = coaches_with_points.groupBy("coach_name", "country").agg(
        _sum("points").alias("total_points"),
        _sum(when(col("medal_type") == "GOLD", 1).otherwise(0)).alias("gold_count"),
        _sum(when(col("medal_type") == "SILVER", 1).otherwise(0)).alias("silver_count"),
        _sum(when(col("medal_type") == "BRONZE", 1).otherwise(0)).alias("bronze_count")
    )

    ordered_coaches = coach_points.orderBy(
        col("country").asc(),
        col("total_points").desc(),
        col("gold_count").desc(),
        col("silver_count").desc(),
        col("bronze_count").desc(),
        col("coach_name").asc()
    )

    china_coaches = ordered_coaches.filter(col("country") == "CHINA").limit(5)
    india_coaches = ordered_coaches.filter(col("country") == "INDIA").limit(5)
    usa_coaches = ordered_coaches.filter(col("country") == "USA").limit(5)

    final_coaches = china_coaches.union(india_coaches).union(usa_coaches)

    athlete_points = medals_with_points.groupBy("athlete_id").agg(
        count(when(col("medal_type") == "GOLD", True)).alias("gold_count"),
        count(when(col("medal_type") == "SILVER", True)).alias("silver_count"),
        count(when(col("medal_type") == "BRONZE", True)).alias("bronze_count"),
        _sum("points").alias("total_points")
    )

    window = Window.partitionBy("sport").orderBy(
        col("total_points").desc(),
        col("gold_count").desc(),
        col("silver_count").desc(),
        col("bronze_count").desc(),
        col("name").asc()
    )

    best_athletes = athlete_points.join(
        athletes_df, 
        athlete_points.athlete_id == athletes_df.id,
        how="inner"
    ).drop(athletes_df.id)

    best_athletes_ranked = best_athletes.withColumn("rank", dense_rank().over(window))

    top_athletes = best_athletes_ranked.filter(col("rank") == 1).dropDuplicates(["name"])

    sorted_athletes = top_athletes.orderBy("sport").select("name").rdd.flatMap(lambda x: x).collect()
    athlete_names = ', '.join([f'"{name}"' for name in sorted_athletes])

    top_coaches = final_coaches.select("coach_name").rdd.flatMap(lambda x: x).collect()
    coach_names = ', '.join([f'"{name}"' for name in top_coaches])

    output = f'([{athlete_names}], [{coach_names}])'
    with open(sys.argv[6], "w") as f:
        f.write(output)

    spark.stop()

if __name__ == "__main__":
    main()

