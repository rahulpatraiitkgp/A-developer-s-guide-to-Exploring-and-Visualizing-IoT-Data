
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook. Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# In[ ]:

#Please don't modify this function
def readDataFrameFromCloudant(host,user,pw,database):
    cloudantdata=spark.read.format("com.cloudant.spark").     option("cloudant.host",host).     option("cloudant.username", user).     option("cloudant.password", pw).     load(database)

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# Sampling is one of the most important things when it comes to visualization because often the data set get so huge that you simply
# 
# - can't copy all data to a local Spark driver (Data Science Experience is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[ ]:

def getSample(df,spark):
    df = df.rdd.sample(False,0.1).toDF()
    return df


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and retur a python list containing all temperature values from the data set

# In[ ]:

def getListForHistogramAndBoxPlot(df,spark):
    result_df = spark.sql("""
    select hardness,temperature,flowrate from washing
    where hardness is not null and 
    temperature is not null and 
    flowrate is not null
    """)
    result_rdd = result_df.rdd.map(lambda row : (row.hardness,row.temperature,row.temperature))
    result_array_temperature = result_rdd.map(lambda (hardness,temperature,flowrate): temperature).collect()
    return result_array_temperature


# Finally we want to create a run chart. Please return two lists (encapusalted in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refere to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[ ]:

#should return a tuple containing the two lists for timestamp and temperature
#please make sure you take only 10% of the data by sampling
#please also ensure that you sample in a way that the timestamp samples and temperature samples correspond (=> call sample on an object still containing both dimensions)
def getListsForRunChart(df,spark):
    result = spark.sql("select temperature,ts from washing where temperature is not null")
    result_rdd = result.rdd.sample(False,0.1).map(lambda row : (row.ts,row.temperature))
    result_array_ts = result_rdd.map(lambda (ts,temperature): ts).collect()
    result_array_temperature = result_rdd.map(lambda (ts,temperature): temperature).collect()
    return (result_array_ts,result_array_temperature)


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# In[ ]:

#TODO Please provide your Cloudant credentials here
hostname = "5da95359-f025-45b3-a14f-1f073c4e2b86-bluemix.cloudant.com"
user = "5da95359-f025-45b3-a14f-1f073c4e2b86-bluemix"
pw = "7848eceb555a38cd95223ee326857466fe69d813e5dcbda03b0ccbe39e071b2d"
database = "washing"
cloudantdata=readDataFrameFromCloudant(hostname, user, pw, database)


# In[ ]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt


# In[ ]:

plt.hist(getListForHistogramAndBoxPlot(cloudantdata,spark))
plt.show()


# In[ ]:

plt.boxplot(getListForHistogramAndBoxPlot(cloudantdata,spark))
plt.show()


# In[ ]:

lists = getListsForRunChart(cloudantdata,spark)
lists


# In[ ]:

plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! Please download the notebook as python file, name it assignment4.1.py and sumbit it to the grader.
