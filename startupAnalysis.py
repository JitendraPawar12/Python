from pyspark import SparkConf,SparkContext
from pyspark.sql.types import * 
from pyspark.sql.functions import * 


conf=SparkConf().setAppName("Daily Revenue").setMaster("yarn-client")
sc=SparkContext(conf=conf)

startupDF = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/user/abhirajs25/jitendra/startup/startup.csv')
####Replace NULLs in 'AmountInUSD' with 0
startupDFgen=startupDF.na.fill('0','AmountInUSD').coalesce(1).select('Date','StartupName','IndustryVertical','SubVertical','CityLocation','InvestorsName','InvestmentType','AmountInUSD','Remarks')
###Convert AmountInUSD to INTEGER
startupDFgen = startupDFgen.withColumn('AmountInUSD', regexp_replace('AmountInUSD', ',', '').cast('integer'))
###City wise funding amount
citywiseFundDF=startupDFgen.select('CityLocation','AmountInUSD').groupBy(['CityLocation']).agg(sum(startupDFgen["AmountInUSD"]).alias("Funding total per city"))
###city wise startups count
citywisefirms=startupDFgen.select('CityLocation','StartupName').groupBy(['CityLocation']).agg(count('StartupName').alias("City wise startup counts"))
###City having maximum startups
--city wise count
maxstartupCity=startupDFgen.select('CityLocation','StartupName').groupBy(['CityLocation']).agg(count('StartupName').alias("City_wise_startup_counts"))
--max count
maxstartupCity1=maxstartupCity.select('CityLocation','City_wise_startup_counts').agg(max('City_wise_startup_counts').alias("max_startups_num_country"))
maxstartupCity1Join=maxstartupCity.join(maxstartupCity1,maxstartupCity.City_wise_startup_counts==maxstartupCity1.max_startups_num_country).select('CityLocation','max_startups_num_country')
###year in which max startup happened
yearwiseFundDF=startupDFgen.withColumn("Date", expr("reverse(substr(reverse(Date),1,4))")).groupBy(["Date"]).agg(count('StartupName').alias("year_wise_startup_count"))
yearwiseFundDF1=yearwiseFundDF.select('Date','year_wise_startup_count').agg(max('year_wise_startup_count').alias("year_wise_max_count"))
yearwiseFundDF1Join=yearwiseFundDF.join(yearwiseFundDF1,yearwiseFundDF.year_wise_startup_count==yearwiseFundDF1.year_wise_max_count).select('Date','year_wise_max_count')
###Industry vertical wise startups and funds
vertwise=startupDFgen.select('IndustryVertical','StartupName','AmountInUSD').groupBy('IndustryVertical').agg(sum("AmountInUSD").alias("Vertical_wise_fund"),count('StartupName').alias("vertical_wise_count"))
###City wise funding
citywiseFundDF=startupDFgen.select('CityLocation','AmountInUSD').groupBy(['CityLocation']).agg(sum(startupDFgen["AmountInUSD"]).alias("Funding_total_per_city"))


###Saving the data:


citywiseFundDF.coalesce(1).write.option("header",True).csv("/user/abhirajs25/jitendra/Startup/citywiseFundDF.csv")
citywisefirms.coalesce(1).write.option("header",True).csv("/user/abhirajs25/jitendra/Startup/citywisefirmscount.csv")
vertwise.coalesce(1).write.option("header",True).csv("/user/abhirajs25/jitendra/Startup/vertwisedata.csv")


