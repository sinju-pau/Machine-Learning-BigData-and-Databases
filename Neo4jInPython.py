
# coding: utf-8

# # Working with Neo4j, The Graph based Database in Python
# 
# Neo4j is the world's leading open source Graph Database which is developed using Java technology. It is highly scalable and schema free (NoSQL). Graph database is a database used to model the data in the form of graph. In here, the nodes of a graph depict the entities while the relationships depict the association of these nodes. The data model for graph databases is simpler compared to other databases and, they can be used with OLTP systems. They provide features like transactional integrity and operational availability.
# 
# This notebook illustrates how to work with Neo4j from Python, using the py2neo driver installed. The files used contains information about names of some cities in US,their populations and flight distances between them. These cities are the entities/nodes we create and the flight distances are the edges/relationships we link.

# Import modules graph, node, realtionship and authenticate from py2neo

# In[1]:

from py2neo import Graph, Node, Relationship, authenticate


# Next is the Authentication process, with host, username and password

# In[2]:

authenticate("localhost", "neo4j", "Jubaleldo2016")


# ## Create Graph and nodes

# Now lets create a graph using graph command

# In[3]:

g = Graph(host='localhost')
type(g)


# Now lets type in a terminal command ls to list the .txt files in the directory

# In[5]:

get_ipython().system('ls *.txt')


# Lets look at the first sevral lines of node_city.txt file

# In[6]:

get_ipython().system('head node_city.txt')


# Now we create a transactions and within the transaction, we load the nodes from the above file into the graph just created.

# In[8]:

tx = g.begin()
with open('node_city.txt') as f_in:
    for line in f_in:
        city_list = line.rstrip().split(',')
        city = Node("City",name=city_list[0], population=int(city_list[1]))
        tx.create(city)
tx.commit()


# In above, .begin( ) begins the transactions and .commit( ) saves it. In between, source file is open and data is read line-by-line, after stripping off whitespaces and separated by commas and converted into a list, city_list.
# 
# Next a node, named "City" is created with the 'Node' command in py2neo module. The attributes are name and population using first and second columns from the list just created. This step is completed with .create( ), the node is said to be of type, city.

# ## Read data from graph

# Data is read using the MATCH RETURN statement as follows. Match all nodes with the type 'City'

# In[10]:

g.data("MATCH (c:City) RETURN c.name, c.population LIMIT 10")


# If we want to work with specific elements of a list, for example, specific cities. One way to do that is to work with something called node_selector by referencing the graph as g and use .select( ) command to select nodes which are of type City.

# In[12]:

result_set = g.node_selector.select("City")
type(result_set)


# Observe that the result is a NodeSelection. to look at the first element,

# In[13]:

result_set.first()


# Let's say we're interested in finding the node for Los Angeles, then use the 'where' clause

# In[19]:

LA = result_set.where(name="Los_Angeles")
LA.first()


# Also for loops can also be used to iterate over the entire entries.

# In[21]:

for r in result_set:
    print(r)


# ## Creating edges/relations between nodes

# Now we create edges/relations using the edge_distance.txt file, containing information about the flight distances between cities.

# In[24]:

get_ipython().system('head edge_distance.txt')


# Into the same graph, g, we now now add edges to nodes, reading data from the open file after formatting. Two nodes are created for the two cities and then edges are created between these two, using Relationships( ). The relationship has a property assigned as 'distance'

# In[23]:

tx = g.begin()
with open('edge_distance.txt') as f_in:
    for line in f_in:
        edge_list = line.rstrip().split(',')
        city1_name = edge_list[0]
        city2_name = edge_list[1]
        city1_node = result_set.where(name=city1_name)
        city2_node = result_set.where(name=city2_name)
        city_pair = Relationship(city1_node, "FLIGHT_BETWEEN", city2_node)
        city_pair["distance"] = edge_list[2]
        tx.create(city_pair)
tx.commit()


# The above creates a edge/relationship of type city_pair

# ## Mapping into pandas dataframe

# We now map the graph structure into a pandas dataframe.

# In[25]:

import pandas as pd


# Now we create a list flights_list and iterate over cities to get a set of cities in the graph that match cases where the relationship type is equal to FLIGHT_BETWEEN. city1_name and city2_name contains the first and second properties of nodes, distance contains the distance property. These are then appended to the flights_list.

# In[26]:

flight_list = list()
for cities in g.match(rel_type="FLIGHT_BETWEEN"):
    city1_name = cities.nodes()[0]['name']
    city2_name = cities.nodes()[1]['name']
    distance = cities['distance']
    flight_list.append([city1_name, city2_name, distance])


# In[27]:

flight_list[0]


# The list is thus created from Neo4j database that we built. Now, this can be easily conveted into a pandas dataframe as follows,

# In[28]:

flight_df = pd.DataFrame(flight_list)


# In[30]:

flight_df.head(10)


# ## Concluding Remarks
# 
# 1. Graphs and Nodes are created in the Neo4j database. Data reading from the graphs is also done
# 2. Edges are created between nodes in the graph and graph is also converted into pandas dataframe.
