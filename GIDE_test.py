import os
import pandas as pd
if __name__ == "__main__":
    cwd = os.getcwd()
    path = cwd + "\GIDE_1.82_20201002_.csv"
    var = 'totaltradeawithb'  #set variable name

    #read file


    data = pd.read_csv(path)

    print(data.head())