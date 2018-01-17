
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.
# 
# YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
# Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
# . Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# In[1]:

#Please don't modify this function
def readDataFrameFromCloudant(host,user,pw,database):
    cloudantdata=spark.read.format("com.cloudant.spark").     option("cloudant.host",host).     option("cloudant.username", user).     option("cloudant.password", pw).     load(database)

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.

# In[2]:

def minTemperature(df,spark):
    from pyspark.sql import functions as F
    val = df.agg(F.min(df.temperature)).collect()
    val = val[0][0]
    return val


# Please now do the same for the mean of the temperature

# In[3]:

def meanTemperature(df,spark):
    val = df.agg({"temperature": "mean"}).collect()
    val = val[0][0]
    return val


# Please now do the same for the maximum of the temperature

# In[4]:

def maxTemperature(df,spark):
    from pyspark.sql import functions as F
    val = df.agg(F.max(df.temperature)).collect()
    val = val[0][0]
    return val


# Please now do the same for the standard deviation of the temperature

# In[5]:

def sdTemperature(df,spark):
    val = df.agg({'temperature': 'stddev'}).collect()
    val = val[0][0]
    return val


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[6]:

def skewTemperature(df,spark):    
    val = df.agg({'temperature': 'skewness'}).collect()
    val = val[0][0]
    return val


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[7]:

def kurtosisTemperature(df,spark):
    val = df.agg({'temperature': 'kurtosis'}).collect()
    val = val[0][0]
    return val  


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[72]:

def correlationTemperatureHardness(df,spark):
    df = df.filter(df.temperature.isNotNull())
    corr = df.corr('hardness','temperature')
    return corr


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# In[9]:

#TODO Please provide your Cloudant credentials here
hostname = "5da95359-f025-45b3-a14f-1f073c4e2b86-bluemix.cloudant.com"
user = "5da95359-f025-45b3-a14f-1f073c4e2b86-bluemix"
pw = "7848eceb555a38cd95223ee326857466fe69d813e5dcbda03b0ccbe39e071b2d"
database = "washing"
cloudantdata=readDataFrameFromCloudant(hostname, user, pw, database)


# In[10]:

minTemperature(cloudantdata,spark)


# In[11]:

meanTemperature(cloudantdata,spark)


# In[13]:

maxTemperature(cloudantdata,spark)


# In[14]:

sdTemperature(cloudantdata,spark)


# In[17]:

skewTemperature(cloudantdata,spark)


# In[18]:

kurtosisTemperature(cloudantdata,spark)


# In[73]:

correlationTemperatureHardness(cloudantdata,spark)

