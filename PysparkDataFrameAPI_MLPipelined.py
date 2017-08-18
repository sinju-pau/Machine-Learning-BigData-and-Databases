
# coding: utf-8

# # Apache Spark in Python using DataFrame API
# 
# Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib and ML for machine learning, GraphX for graph processing, and Spark Streaming.
# 
# This notebook uses a  machine learning problem based upon a dataset available at: http://www.sgi.com/tech/mlc/db/churn.data. 
# 
# In the targeted approach the company tries to identify in advance customers who are likely to churn. The company then targets those customers with special programs or incentives. This approach can bring in huge loss for a company, if churn predictions are inaccurate, because then firms are wasting incentive money on customers who would have stayed anyway. There are numerous predictive modeling techniques for predicting customer churn. The task here is to predict whether the customer will churn or not, using the given features.
# 
# The data files state that the data are "artificial based on claims similar to real world". These data are also contained in the C50 R package. This analysis make use of the DataFrame based API based on the pyspark.ml library. Read more on DataFrame based Spark API at : http://spark.apache.org/docs/latest/sql-programming-guide.html

# Begin by importing the pyspark libraries and associated classes. We have imported the Pandas as well.

# In[1]:

import pyspark 
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pandas as pd


# Now lets build a Spark session using the session builder as below. Alternatively, we can create a SparkContext object.
# 
# sc = pyspark.SparkContext( )
# 
# sqlContext = SQLContext(sc)

# ## Start Spark session

# In[2]:

# start Spark session
spark = pyspark.sql.SparkSession.builder.appName('Churn').getOrCreate()


# Now that the Spark session object is created, we now read in the dataset as follows.

# In[3]:

# load churn_data.csv into Spark dataframe using Pandas
df = spark.createDataFrame(pd.read_csv('churn_data.csv', names=["state","account_length","area_code","phone_number","international_plan","voice_mail_plan","number_vmail_messages","total_day_minutes","total_day_calls","total_day_charge",
                                                                "total_eve_minutes","total_eve_calls","total_eve_charge","total_night_minutes","total_night_calls","total_night_charge","total_intl_minutes","total_intl_calls","total_intl_charge",
                                                                "number_customer_service_calls","churned"]))


# Lets view the first few rows of the DataFrame, df so created.

# In[4]:

df.show(5)


# ## Data Preprocessing

# Now choose relevant input features for our analysis, with "churned" as the target feature

# In[5]:

df = df["account_length","number_vmail_messages","total_day_minutes","total_day_calls","total_day_charge","total_eve_minutes","total_eve_calls","total_eve_charge","total_night_minutes","total_night_calls","total_night_charge","total_intl_minutes","total_intl_calls","total_intl_charge","number_customer_service_calls","churned"]


# Now lets print the schema

# In[6]:

df.printSchema()
df.count()


# The dataframe now has 3333 rows of features. Now lets split the data into training and test sets.

# In[7]:

training_data, test_data = df.randomSplit([0.7, 0.3], seed = 0)


# ### Machine Learning Pipelining
# 
# Lets work with the Spark DataFrames by making use of the extensive pipelining feature available. Pipelining of DataFrames involves the following stages. Lets move on to them one by one.

# 1.Converting categorical attribute label indexes into numeric
# 
# 2.Encoding numerics into numerical vectors
# 
# 3.Combining all numerical vectors into a single feature vector and making the pipeline
# 
# 4.Fitting a ML model on the extracted features
# 
# 5.Predicting using ML model to get outputs for each data row

# ### 1. Converting categorical attribute labels into label indexes
# 
# This is done making use of the StringIndexer class, which converts the categorical output into numerics.

# In[8]:

from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol='churned', outputCol='churned_numeric').fit(df)
indexed_df = indexer.transform(training_data)
indexed_df.show(5)


# ### 2. Encoding numerics into numerical vectors
# 
# This is done making use of the OneHotEncoder class, which converts the numerics into vectors.

# In[9]:

from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCol="churned_numeric", outputCol="churned_vector")
encoded_df = encoder.transform(indexed_df)
encoded_df.show(5)


# ### 3. Combining all numerical vectors into a single feature vector
# 
# This is done using the VectorAssembler

# In[10]:

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=['churned_vector', 'account_length',
              "number_vmail_messages","total_day_minutes","total_day_calls","total_day_charge",
               "total_eve_minutes","total_eve_calls","total_eve_charge","total_night_minutes","total_night_calls",
               "total_night_charge","total_intl_minutes","total_intl_calls","total_intl_charge","number_customer_service_calls"], 
    outputCol="features")

final_df = assembler.transform(encoded_df)
final_df.show(5)


# ### Building the pipeline
# Lets now build the pipeline , combining all the above three stages, on the training dataset.

# In[11]:

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[indexer, encoder, assembler])
pipelined_model = pipeline.fit(training_data)
pipelined_training_data = pipelined_model.transform(training_data)
pipelined_training_data.show(5)


# ### Fitting the Logistic Regression model
# 
# Lets now fit the Logistic Regression model using the input features and labels. We also apply some regularization parameters to the model.

# In[12]:

lr = LogisticRegression(labelCol="churned_numeric", featuresCol="features",
                        maxIter=10, regParam=0.3, elasticNetParam=0.8,family = 'multinomial')


# In[13]:

# Fit the model
lrModel = lr.fit(pipelined_training_data)


# Print the coefficients and intercepts for the two-class logistic regression 

# In[14]:

print("coefficients: " + str(lrModel.coefficientMatrix))
print("intercepts: " + str(lrModel.interceptVector))


# ### Making the predictions
# 
# We now make predictions using the transformed test dataset.

# In[15]:

# Make predictions
pipelined_test_data = pipelined_model.transform(test_data)
pipelined_test_data.show(5)


# In[16]:

predictions = lrModel.transform(pipelined_test_data)


# The above returns a prediction DataFrame containing the vector of predictions and actual labels.

# In[17]:

# Select example rows to display.
predictions.select("prediction","churned_numeric", "features").show(5)


# Now make use of the Multiclass. Evaluator to compute the accuracy.

# In[18]:

# Select (prediction, true label) and compute accuracy
evaluator = MulticlassClassificationEvaluator(labelCol = 'churned_numeric',
                                              predictionCol = 'prediction',
                                              metricName = 'accuracy')
accuracy = evaluator.evaluate(predictions)
print("Accuracy = ", accuracy)


# ### Cross Validation
# Now lets perform a simple cross validation using the CrossValidator class. The parameters of the model built are: 

# In[19]:

print(lr.explainParams())


# We choose the parameters regParam, elasticNetParam and maxIter for our analysis

# In[20]:

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Create ParamGrid for Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.01, 0.5, 2.0])
             .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
             .addGrid(lr.maxIter, [1, 5, 10])
             .build())


# In[21]:

cv = CrossValidator(estimator=lr, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# Run cross validations
cvModel = cv.fit(pipelined_training_data)


# This will likely take a fair amount of time because of the amount of models that we're creating and testing

# In[22]:

# Use test set here so we can measure the accuracy of our model on new data
predictions = cvModel.transform(pipelined_test_data)


# cvModel uses the best model found from the Cross Validation. Evaluate best model

# In[23]:

evaluator.evaluate(predictions)


# ## Concluding Remarks
# 1. A Spark session is created, data is read in and transformed using ML pipelining 
# 2. A Machine Learning model is created, predictions are made on the test data and evaluated accuracies
