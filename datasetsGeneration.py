import numpy as np 
import matplotlib.pyplot as plt
import sys
import random

######################################################################################

#                       The code takes 3 arguments as inputs:

#    1 = Point-dimensionality,   2 = Number of points,    3 = Distribution








################################# Covariance Matrix Creation for Correlated And Anticorrelated Distributions ############################################################

values = [10,15,20,27,22]

correlated = [0.9, 0.8, 0.7, 0.6]

anticorellated = [-0.9, -0.8, -0.7, -0.6]

if sys.argv[3] == "Correlated":
    x = [random.choice(correlated) for _ in range(0, int(sys.argv[1]))]

if sys.argv[3] == "Anticorrelated":
    x = [random.choice(anticorellated) for _ in range(0, int(sys.argv[1]))]



mean = [random.choice(values) for _ in range(0, int(sys.argv[1]))]


def covarianceMatrixCreation():
    d = []
    for i in range(0, int(sys.argv[1])):
        if i == 0:
            d.append(x)
        else:
            d.append(helpFunction(i))
    return d


def helpFunction(index):
    d = []
    for i in range(0, int(sys.argv[1])):
        if i == 0:
            d.append(x[index])
        elif i == index:
            d.append(1)
        else:

            d.append(0.5)
    return d

  
##################################################################################################################



def correlated(dimension:int, size:int):    # Create Correlated distribution dataset
    covarianceMatrix = np.array(covarianceMatrixCreation())
    m = np.random.multivariate_normal(mean,covarianceMatrix,size, "ignore")
    np.savetxt(str(size) + ".txt", m)


def anticorrelated(dimension:int, size:int):    # Create Anticorrelated distribution dataset
    covarianceMatrix = np.array(covarianceMatrixCreation())
    m = np.random.multivariate_normal(mean,covarianceMatrix,int(size), "ignore")
    np.savetxt(str(size) + ".txt", m)


def uniform(dimension:int, size:int):   # Create Uniform distribution dataset
    m = np.random.uniform(low = 10, high = 30, size = [size,dimension])
    np.savetxt(str(size) + ".txt", m)


def normal(dimension:int, size:int):    # Create Normal distribution dataset
    m = np.random.normal(loc=30, scale=10, size = [size,dimension])
    np.savetxt(str(size) + ".txt", m)




def main(distribution:str, dimension:int, size:int):
    if distribution == "Correlated":
        correlated(dimension, size)
    if distribution == "Anticorrelated":
        anticorrelated(dimension, size)
    if distribution == "Uniform":
        uniform(dimension, size)
    if distribution == "Normal":
        normal(dimension, size)




if __name__ == "__main__":
    main(distribution=sys.argv[3],dimension=int(sys.argv[1]),size=int(sys.argv[2]))
