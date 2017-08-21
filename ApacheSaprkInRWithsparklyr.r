
#install.packages("sparklyr") 

library(sparklyr) 
#spark_install(version = "1.6.2")

sc <- spark_connect(master = "local")

install.packages("nycflights13")

install.packages("Lahman")

library(dplyr) 
iris_tbl <- copy_to(sc, iris) 
flights_tbl <- copy_to(sc, nycflights13::flights, "flights") 
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")

# filter by departure delay 
flights_tbl %>% filter(dep_delay == 2)

delay <- flights_tbl %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect()

# plot delays
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)

batting_tbl %>%   
  select(playerID, yearID, teamID, G, AB:H) %>%   
  arrange(playerID, yearID, teamID) %>%   
  group_by(playerID) %>%   
  filter(min_rank(desc(H)) <= 2 & H > 0)

#install.packages("DBI")
library(DBI) 
iris_preview <- dbGetQuery(sc, "SELECT * FROM iris LIMIT 10") 
iris_preview

# copy mtcars into spark
mtcars_tbl <- copy_to(sc, mtcars)

# transform our data set, and then partition into 'training', 'test'
partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

# fit a linear model to the training dataset
fit <- partitions$training %>%
  ml_linear_regression(response = "mpg", features = c("wt", "cyl"))

summary(fit)

# convert to h20_frame (uses the same underlying rdd)
library(h2o)
h2o.init()

training <- as.h2o(partitions$training)
test <- as.h2o(partitions$test)

# fit a linear model to the training dataset
fit <- h2o.glm(x = c("wt", "cyl"),
               y = "mpg",
               training_frame = training)

summary(fit)
