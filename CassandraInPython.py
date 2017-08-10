
# coding: utf-8

# # Working with Cassandra in Python
# 
# Cassandra is a distributed database from Apache that is highly scalable and designed to manage very large amounts of structured data. It provides high availability with no single point of failure. Cassandra offers robust support for clusters spanning multiple datacenters, with asynchronous masterless replication allowing low latency operations for all clients.
# 
# This notebook illustrates how to work with Cassandra's basic operations such as creating clusters and loading data, using Python. Cassandra needs to be installed and running in the system. The data for the database is taken from : https://www.kaggle.com/usfundamentals/us-stocks-fundamentals# which contains a combination of three files for US stocks fundamentals.

# Lets begin by importing the cassandra module, Cluster as follows

# In[1]:

from cassandra.cluster import Cluster


# ## Create a connection to the cluster

# Now lets create a connection to the cluster by creating a cluster object and calling the Cluster command.
# 
# The 'Cluster' command expects a list of hosts, so the code below connects to the cluster on 'localhost'.

# In[2]:

cluster = Cluster(['localhost'])


# Now,'cluster' is an object representing the cluster.
# 
# Again, lets create a session variable,reference the cluster variable just created and ask it to give the connection.

# In[3]:

session = cluster.connect()


# This completes the connection steps.

# ## Create a keyspace

# Keyspace is a top level data structure in Cassandra. To create keyspace, lets use '.execute' command on the session object. This is done making use of CQL (Cassandra Query Language) syntax, very similar to SQL.

# In[4]:

session.execute("CREATE KEYSPACE stocks                    WITH replication = {'class':'SimpleStrategy',                                         'replication_factor':'1'}")


# This creates a KEYSPACE called stocks. Now, to tell Cassandra that this is the keyspace that we want to work with,

# In[5]:

session.set_keyspace('stocks')


# ## Creating Tables

# This is similar to setting up databases in MySQL or Schemas in Oracle. This is done with session object as follows.

# In[6]:

session.execute("""
                  CREATE TABLE company (
                      company_id text,
                      name_latest text,
                      name_previous text,
                      PRIMARY KEY (company_id)
                   )
                """)


# The above code creates a 'table' with the name 'company' with 'company_id', 'name_latest', 'name_previous' as columns and primary key as 'comapny_id'.
# 
# Also lets create another table 'indicator_by_company'

# In[7]:

session.execute("""
                  CREATE TABLE indicator_by_company (
                      company_id text,
                      indicator_id text,
                      yr_2010 bigint,
                      yr_2011 bigint,
                      yr_2012 bigint,
                      yr_2013 bigint,
                      yr_2014 bigint,
                      yr_2015 bigint,
                      yr_2016 bigint,
                      PRIMARY KEY (company_id, indicator_id)
                   )
                """)


# Observe that the table above is highly denormalized, and this is a typical pattern that we see in Cassandra. It's called wide table, because it supports this kind of wide-column normalization. 'bigint' supports large integer values.

# ## Loading data into Cassandra

# Lets load data from 'companies.json' file into Cassandra table 'company'.

# In[8]:

import json


# Open the json file and load data into a variable companies.

# In[9]:

with open('companies.json') as f_in:
    companies = json.load(f_in)


# This creates a list of dictionaries ,companies.

# In[10]:

type(companies)


# In[11]:

type(companies[0])


# Now execute a very simple insert statement to insert one company record into the table 'company' already created.

# In[12]:

session.execute(
   """
   INSERT INTO company (company_id, name_latest, name_previous)
   VALUES (%s, %s, %s)
   """,
   ("1000045", "Nicholas Financial Inc", ""))


# To check the inserted values,

# In[13]:

result_set = session.execute("SELECT * FROM company")
result_set.current_rows


# The data in one row is well inserted. The entire data can be inserted using a for loop as follows.

# In[14]:

for company in companies:
    try:
        session.execute(
                           """
                           INSERT INTO company (company_id, name_latest, name_previous)
                           VALUES (%s, %s, %s)
                           """,
                           (company['company_id'],company['name_latest'], company['names_previous']))
    except:
        pass


# In[15]:

result_set = session.execute("SELECT * FROM company")
result_set.current_rows


# The entire data is now inserted into the table 'company'.  The keyspace 'stocks can be deleted as follows using DROP KEYSPACE. 

# In[16]:

KEYSPACE = 'stocks'
session.execute("DROP KEYSPACE " + KEYSPACE)


# ## Cocluding remarks
# 
# 1. Creating cluster, establishing connections via sessions, creating keyspaces and tables is illustrated.
# 2. Insertion of data into a table from a json file as well as deletion of keyspace is also shown.
