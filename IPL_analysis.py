from pyspark import SparkConf, SparkContext 
from pyspark.sql import SQLContext,SparkSession,DataFrameReader 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

conf = SparkConf().setMaster("local").setAppName("IPL_Analysis")
sc = SparkContext(conf = conf)

spark = SparkSession \
        .builder \
        .appName("IPL_Analysis") \
        .config(conf = conf) \
        .getOrCreate()

DataDF = spark.read.csv ("/home/abhirajs25/IPL.csv", header=True)		

DataDF.show()

InningsDF = DataDF.coalesce(1).select('match_id','batting_team','bowling_team','batsman','player_dismissed').distinct().filter("batting_team=='Royal Challengers Bangalore'").groupBy(['match_id','batting_team','bowling_team','batsman']).agg(count("*").alias("out")).filter("out == 2").groupBy(['batting_team','batsman']).agg(count("*").alias("No_Of_Innings"))

InningsDF.show()

BallsPlayedDF=DataDF.coalesce(1).select('match_id','batting_team','bowling_team','batsman','player_dismissed','ball').filter("batting_team=='Royal Challengers Bangalore'").groupBy(['match_id','batting_team','bowling_team','batsman']).agg(count("*").alias("Ball Played"))

BallsPlayedDF.show()

PerMatchRunsBatDF=DataDF.coalesce(1).select('match_id','batting_team','bowling_team','batsman','batsman_runs').filter("batting_team=='Royal Challengers Bangalore'").groupBy(['match_id','batting_team','bowling_team','batsman']).agg(sum(DataDF["batsman_runs"]).alias("Totals runs per batsman per match"))

PerMatchRunsBatDF.show()

Average_per_season = PerMatchRunsBatDF.coalesce(1).groupBy(['batting_team','batsman']).agg(sum(PerMatchRunsBatDF["Totals runs per batsman per match"]).alias("Total_Runs")).join(InningsDF.coalesce(1),['batting_team','batsman'])

Average_per_season.show()

Batting_Average_per_Batsman = Average_per_season.withColumn('Batting_Average',Average_per_season.Total_Runs/Average_per_season.No_Of_Innings)

Batting_Average_per_Batsman.show()

PerMatchRunsBatDF.coalesce(1).write.option("header",True).csv("/home/abhirajs25/Total_Runs_per_Batsman_per_Match_script1.csv")

PerMatchRunsBatDF.show()

Batting_Average_per_Batsman.coalesce(1).write.option("header",True).csv("/home/abhirajs25/Batting_Average_per_batsman_script1.csv")

Batting_Average_per_Batsman.show()


