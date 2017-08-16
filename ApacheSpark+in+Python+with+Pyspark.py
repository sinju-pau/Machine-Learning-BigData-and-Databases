
# coding: utf-8

# # Apache Spark in Python with PySaprk

# Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph processing, and Spark Streaming.
# 
# In this notebook, we'll train two classifiers to predict survivors in the Titanic dataset. The dataset us available form : https://www.kaggle.com/c/titanic/data. A modified version of the data has been used for our analysis.
# 
# We'll use this classic machine learning problem as a brief introduction to using Apache Spark local mode in a notebook. Read more on Spark Programming at :http://spark.apache.org/docs/latest/rdd-programming-guide.html

# In[1]:

import pyspark  
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.tree import DecisionTree


# First we create a [SparkContext](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.SparkContext), the main object in the Spark API. This call may take a few seconds to return as it fires up a JVM under the covers. SparkContext represents a connection to the Spark Cluster.

# In[2]:

sc = pyspark.SparkContext()


# ## Sample the data

# We point the context at a CSV file on disk. The result is a RDD, not the content of the file. This is called as a Spark transformation.

# In[3]:

raw_rdd = sc.textFile("titanic.csv")


# We query RDD for the number of lines in the file. The call here causes the file to be read and the result computed. This is a called as a Spark action.

# In[4]:

raw_rdd.count()


# Now, lets query for the first five rows of the RDD. Even though the data is small, we should not be pulling the entire dataset into the notebook. Many datasets that we might want to work with using Spark may be much too large to fit in memory of a single machine.

# In[5]:

raw_rdd.take(5)


# We see a header row followed by a set of data rows. We filter out the header to define a new RDD containing only the data rows.

# In[6]:

header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)


# Lets now take a random sample of the data rows to better understand the possible values.

# In[7]:

data_rdd.takeSample(False, 5, 0)


# We see that the first value in every row is a passenger number. The next three values are the passenger attributes we might use to predict passenger survival: ticket class, age group, and gender. The final value is the survival ground truth.

# ## Create labeled points (i.e., feature vectors and ground truth)

# Now we define a function to turn the species attributes into structured `LabeledPoint` objects. This builds a LabelPoint consisting of:
#     
#     survival (truth): 0=no, 1=yes
#     ticket class: 0=1st class, 1=2nd class, 2=3rd class
#     age group: 0=child, 1=adults
#     gender: 0=man, 1=woman
#  

# In[8]:

def row_to_labeled_point(line):
    passenger_id, klass, age, sex, survived = [segs.strip('"') for segs in line.split(',')]
    klass = int(klass[0]) - 1
    
    if (age not in ['adults', 'child'] or 
        sex not in ['man', 'women'] or
        survived not in ['yes', 'no']):
        raise RuntimeError('unknown value')
    
    features = [
        klass,
        (1 if age == 'adults' else 0),
        (1 if sex == 'women' else 0)
    ]
    return LabeledPoint(1 if survived == 'yes' else 0, features)


# We apply the function to all rows.

# In[9]:

labeled_points_rdd = data_rdd.map(row_to_labeled_point)


# We take a random sample of the resulting points to inspect them.

# In[10]:

labeled_points_rdd.takeSample(False, 5, 0)


# ## Split for training and test

# We split the transformed data into a training (70%) and test set (30%), and print the total number of items in each segment.

# In[11]:

training_rdd, test_rdd = labeled_points_rdd.randomSplit([0.7, 0.3], seed = 0)


# In[12]:

training_count = training_rdd.count()
test_count = test_rdd.count()
training_count, test_count


# ## Building a decision tree classifier

# Now we train a [DecisionTree](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.tree.DecisionTree) model. We specify that we're training a boolean classifier (i.e., there are two outcomes). We also specify that all of our features are categorical and the number of possible categories for each.

# In[13]:

model = DecisionTree.trainClassifier(training_rdd, 
                                     numClasses=2, 
                                     categoricalFeaturesInfo={
                                        0: 3,
                                        1: 2,
                                        2: 2
                                     })


# We now apply the trained model to the feature values in the test set to get the list of predicted outcomes

# In[14]:

predictions_rdd = model.predict(test_rdd.map(lambda x: x.features))


# We bundle our predictions with the ground truth outcome for each passenger in the test set.

# In[15]:

truth_and_predictions_rdd = test_rdd.map(lambda lp: lp.label).zip(predictions_rdd)


# Now we compute the test error (% predicted survival outcomes == actual outcomes) and display the decision tree for good measure.

# In[16]:

accuracy = truth_and_predictions_rdd.filter(lambda v_p: v_p[0] == v_p[1]).count() / float(test_count)
print('Accuracy =', accuracy)


# For a simple comparison, we also train and test a LogisticRegressionWithSGD model.

# In[17]:

model = LogisticRegressionWithSGD.train(training_rdd)


# In[18]:

predictions_rdd = model.predict(test_rdd.map(lambda x: x.features))


# In[19]:

labels_and_predictions_rdd = test_rdd.map(lambda lp: lp.label).zip(predictions_rdd)


# In[20]:

accuracy = labels_and_predictions_rdd.filter(lambda v_p: v_p[0] == v_p[1]).count() / float(test_count)
print('Accuracy =', accuracy)


# The two classifiers show similar accuracy. Adding more information about the passengers could definitely help improve this metric.
