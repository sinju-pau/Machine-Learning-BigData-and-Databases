
# coding: utf-8

# # Basic CRUD operations with MongoDB using Python
# 
# MongoDB is an open source database that uses a document-oriented data model.It is one of several database types to arise in the mid-2000s under the NoSQL banner. Instead of using tables and rows as in relational databases, MongoDB is built on an architecture of collections and documents. Documents comprise sets of key-value pairs and are the basic unit of data in MongoDB. Collections contain sets of documents and function as the equivalent of relational database tables. 
# 
# Like other NoSQL databases, MongoDB supports dynamic schema design, allowing the documents in a collection to have different fields and structures. The database uses a document storage and data interchange format called BSON, which provides a binary representation of JSON-like documents. Automatic sharding enables data in a collection to be distributed across multiple systems for horizontal scalability as data volumes increase.
# 
# PyMongo is a Python distribution containing tools for working with MongoDB, and is the recommended way to work with MongoDB from Python. To use pymongo, we need to install MongoDB first, and then run it. This notebook is an illustration of the basic Create, Read, Update and Delete operations in MongoDB using pymongo.
# 
# The database is created using the Adult dataset from : https://archive.ics.uci.edu/ml/datasets/adult

# Lets begin by loading the libraries

# In[1]:

import pymongo
import pandas as pd


# pymongo is the driver used in Python to work with MongoDB

# ## Creating Collections

# Create an object called client using MongoClient, database using is 'localhost', 27017 is the port number(in my system)that MongoDB is listening on.

# In[2]:

client = pymongo.MongoClient('localhost',27017)


# Create an object 'db' using client and ask it to connect to the database, 'mongo' here. If it does not exist, Mongo driver will create it.

# In[3]:

db = client['mongo']


# In Document databases as in MongoDB, we typically work with collections. So lets create a collection. 
# 
# Tell the driver that we want to connect to something called 'income'. If 'income' doesn't exist, it'll be created.

# In[4]:

collection = db['income']


# Data is saved in 'income.txt' file and headers in income_header.txt' file. Lets now open the income_header.txt file and save the column names

# In[5]:

with open('income_header.txt') as finput:
    column_names = finput.readline()
column_names


# Now create a Python list of column names

# In[6]:

column_names_list = column_names.split(',')
column_names_list


# Now create a dictionary with column_names as 'keys' and observations from income.txt as 'values'.
# Also, lets insert rows line by line to obtain the collection of the dictionaries. Here, the data is converted into JSON (JavaScript Object Notation) format using the Python's dict( ). 
# 
# The character variable 'age' is also converted into integer. Other variables can be converted in a similar fashion (not done).

# In[7]:

with open('income.txt')as finput:
    for line in finput:
        row_list =line.rstrip('\n').split(',')
        row_dict = dict(zip(column_names_list,row_list))
        try:
            row_dict['age']= int(row_dict['age'])
            collection.insert_one(row_dict)
        except:
            pass


# In[8]:

collection.count()


# Thus, a collection of 32,561 items is created. This forms our database for working with MongoDB.

# ## Read operation using find ( ) in MongoDB

# Lets now check our collection using the find_one( ) in MongoDB. This is similar to the SELECT statement in SQL for relational databases.
# 
# Lets lookup for a collection with 'age' = 39 as follows

# In[9]:

age39 = collection.find_one({'age':{'$eq' : 39}}) #alternatively, {'age': 39} can be used
age39


# Also,"$gt" can be used to find collections where 'age' > 35, which is to be put in an embedded list.

# In[10]:

ageover_35 = collection.find({'age':{"$gt" : 35}})
ageover_35.count()


# In[11]:

type(ageover_35)


# The database has 17,636, about half of instances where 'age' > 35. Observe that the 'ageover_35 is a cursor, which fucntions as a pointer to the current instance. To get the next instance,

# In[12]:

ageover_35.next()


# ## Update operation

# Update operation is done using the update_one( ) and update_many( ). The first field accepts the criterion for update and values are updated using the '$set': argument.

# In[13]:

collection.update_one(
        {"age": 38},
        {
        "$set": {
            "capital-gain":999,
            "capital-loss":999,
                }
        })


# In[14]:

updated = collection.find_one({'age':{'$eq' : 38}})
updated


# ### Indexing in MongoDB

# Lets first find, how long it takes to query something using the find ( ) as above without an index, so create a variable called start for keeping track of time, and call the time function in the time package in python

# In[15]:

import time
start = time.time()
ageover50 = collection.find({'age':{'$gt':50}})
end = time.time()
print(end-start)


# Lets now create an index using  profiles.create_index( )

# In[16]:

index_result = db.profiles.create_index([('age',pymongo.ASCENDING)],unique = False)


# In[17]:

import time
start = time.time()
ageover45 = collection.find({'age':{'$gt':45}})
end = time.time()
print(end-start)


# The reduction of time even by such a small amount is highly significant. This is why Indexing is important.

# ## Read data from MongoDB into dataframes

# We convert the cursor ,collection.find( ), into a list and then into a Pandas dataframe using .DataFrame( )

# In[18]:

income_df = pd.DataFrame(list(collection.find()))


# In[19]:

income_df.head()


# In[20]:

income_df['age'].describe()


# The dataframe can now be used to perform all the Data Science and Machine Learning tasks

# ## delete ( ) in MongoDB

# The above created collection can be deleted using delete_many( ) or selectively using delete_one( )

# In[21]:

collection.delete_many({})


# # Concluding Remarks
# 
# 1. The basic CRUD operations are performed for MongoDB using the pymongo driver in Python.
