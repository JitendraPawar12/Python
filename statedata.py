from pyspark import SparkConf, SparkContext 
from pyspark.sql import SQLContext, SparkSession, DataFrameReader 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from pyspark.sql import SQLContext
conf = SparkConf().setMaster("local").setAppName("Startup_analysis")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
spark = SparkSession \
        .builder \
        .appName("Startup_analysis") \
        .config(conf = conf) \
        .getOrCreate()
stateDF=sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/abhirajs25/statedata.csv')		

######city having max population
maxpopcity1=stateDF.select('state_name','name_of_city','population_total')
maxpop=stateDF.select('population_total').agg(max("population_total").alias('max_pop'))
maxpopcity=maxpopcity1.join(maxpop,maxpopcity1.population_total==maxpop.max_pop)
########state having max population
maxpopstate1=stateDF.select('state_name','population_total').groupBy(['state_name']).agg(sum('population_total').alias("statewise_population")).sort(desc("statewise_population")).limit(1)
#####city having max number of females.
maxfpopcity=stateDF.select('state_name','name_of_city','population_female')
maxfpopcnt=maxfpopcity.agg(max("population_female").alias('max_female_pop_city'))
maxfpopcity1=maxfpopcity.join(maxfpopcnt,maxfpopcnt.max_female_pop_city==maxfpopcity.population_female)
####District having max number of female population:
maxfpopdist=stateDF.select('state_name','population_female').groupBy(['state_name']).agg(sum('population_female').alias("dist_wise_female_pop")).sort(desc("dist_wise_female_pop")).limit(1)
####max number of literate females district and its state.
maxnumlitfemales=stateDF.select('state_name','name_of_city','literates_female')
maxnumlitfemales1=stateDF.select('state_name','name_of_city','literates_female').agg(max('literates_female').alias("city_max_lit_pop"))
maxnumlitfemales2=maxnumlitfemales.join(maxnumlitfemales1,maxnumlitfemales1.city_max_lit_pop==maxnumlitfemales.literates_female)
####Best sex ratio.
sexratio=stateDF.select('state_name','name_of_city','sex_ratio')
sexratio1=stateDF.select('state_name','name_of_city','sex_ratio').agg(max('sex_ratio').alias("city_max_sex_ratio"))
sexratio2=sexratio.join(sexratio1,sexratio1.city_max_sex_ratio==sexratio.sex_ratio)
####Worst sex ratio.
sexratio=stateDF.select('state_name','name_of_city','sex_ratio')
sexratio1=stateDF.select('state_name','name_of_city','sex_ratio').agg(min('sex_ratio').alias("city_min_sex_ratio"))
sexratio2=sexratio.join(sexratio1,sexratio1.city_min_sex_ratio==sexratio.sex_ratio)
####literate female to male ratio statewise.
litmalesum=stateDF.select('state_name','literates_male','literates_female').groupBy(['state_name']).agg(sum('literates_male').alias("state_wise_lit_males"))
litfemalesum=stateDF.select('state_name','literates_male','literates_female').groupBy(['state_name']).agg(sum('literates_female').alias("state_wise_lit_females"))
litmalesumjoin=litmalesum.join(litfemalesum,litmalesum.state_name==litfemalesum.state_name).select(litmalesum.state_name,litmalesum.state_wise_lit_males,litfemalesum.state_wise_lit_females,litmalesum.state_wise_lit_males/litfemalesum.state_wise_lit_females)
###percentage of female graduates to mail graduates.
litmalesumgrad=stateDF.select('state_name','male_graduates','female_graduates').groupBy(['state_name']).agg(sum('male_graduates').alias("state_wise_grad_males"))
litfemalegrad=stateDF.select('state_name','male_graduates','female_graduates').groupBy(['state_name']).agg(sum('female_graduates').alias("state_wise_grad_females"))
litmalesumgradjoin=litmalesumgrad.join(litfemalegrad,litmalesumgrad.state_name==litfemalegrad.state_name).select(litmalesumgrad.state_name,litmalesumgrad.state_wise_grad_males,litfemalegrad.state_wise_grad_females,(litmalesumgrad.state_wise_grad_males/litfemalegrad.state_wise_grad_females).alias("Ratio of male to female graduates")).withColumn('percent',(litfemalegrad.state_wise_grad_females/litmalesumgrad.state_wise_grad_males)*100)
litmalesumgradjoin.coalesce(1).write.option("header",True).csv("/home/abhirajs25/StateData2")