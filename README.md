# Titanic-Machine-Learning-from-Diaster
To predict if a passenger survived the sinking of the Titanic or not. For each PassengerId in the test set, must predict a 0 or 1 value for the Survived variable.

The sinking of the RMS Titanic is one of the most infamous shipwrecks in history.  On April 15, 1912, during her maiden voyage, the Titanic sank after colliding with an iceberg, killing 1502 out of 2224 passengers and crew.
This sensational tragedy shocked the international community and led to better safety regulations for ships.

Kaggle has put together an interesting competition on <a href="https://www.kaggle.com/c/titanic">Titanic</a> upon a dataset containing data on who survived and who died on the Titanic. 
The challenge is to build a model that can look at characteristics of an individual who was on the Titanic and predict the likelihood that they would have survived. 

There are several useful variables that they include in the dataset for each person:
1. pclass: passenger class (1st, 2nd, or 3rd)
2. sex
3. age
4. sibsp: number of Siblings/Spouses Aboard
5. parch: number of Parents/Children Aboard
6. fare: how much the passenger paid
7. embarked: where they got on the boat (C = Cherbourg; Q = Queenstown; S = Southampton)

The data files together with a well-detailed description of the variables involved can be found at <a href="https://www.kaggle.com/c/titanic">Titanic</a>

The data analysis is performed using R, in RStudio environment. R is an open-source software environment for statistical computing and graphics. 
It compiles and runs on a wide variety of UNIX platforms, Windows and MacOS.You will need to have R set up on your computer. 
<a href="https://www.r-project.org/">R Installation</a> is free and and an easy one to do.

The dataset is split into a training set and a test set, because the test set is competition-specific.

Step :1
First set the R working directory to the folder that contains data, using the ``` setwd() ```command:

```R

setwd('Users/Desktop/Titanic')

```
Step :2
To load the data available with the file ``` train.csv```, use the ``` read.table()``` function:

```R

data<-read.table("train.csv",sep=",",header=TRUE)

```

This command reads in the file ``` “train.csv”```, using the delimiter ``` “,”```, including the header row as the column names, and assigns it to the R object ``` data ```.

Step :3
Now, take a look at the first few rows of the training set:

```R

head(data)

```
![alt text](screenshots/r1.png "Description goes here")



