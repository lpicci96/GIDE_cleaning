import numpy as np
import pandas as pd
import math
import time

'''
This script fixes dyadic series in the gide dataset.

- If both mirrored values are null, ignore
- If only one value is null, replace value with the mirrored values (regardless of alphabetical order)
- If values for mirrored pairs differ, take the value in alphabetical 
order of countrya and apply it to the mirrored value.

'''

#__________  Initialization _____________
path = r"C:\Users\lpicc\OneDrive\Documents\Pardee work\viz team\gide\GIDE_1.82_20201002.csv"
var = 'totaltradeawithb'  #set variable name

#read file
data = pd.read_csv(path)
start_time = time.time()
print('---', var, '---')

#________________________________________
### Fix Null Values
nulls = data[data[var].isnull()].index.tolist() # make list of null value index

for i in nulls:
    condition = ((data.countryb == data.loc[i, :][0])& 
                 (data.countrya == data.loc[i, :][1]) & 
                 (data.year == data.loc[i, :][2]))
    if data.loc[condition, var].isnull().any():  #ignore if mirrored value is also null
        continue
    else:
        data.loc[i, var] = data.loc[condition, var].values[0] # replace null with mirror value
#________________________________________        
### Fix Non-Null Values     
years = data.year.unique()
for year in years:
    print(year)
    countries = data.loc[data.year == year, 'countrya'].unique()
    used = [] #empty list used to store fixed country pairs
    for countrya in countries:
        for countryb in countries:
            pair = [countrya, countryb]
            if countrya == countryb:
                continue
            #check if pairs have been used
            elif pair in used:
                continue
            #check if reverse pair has been corrected
            elif [countryb, countrya] in used:
                continue
                
            #else fix values that exist
            else:
                condition_a = (data.countrya == countrya) & (data.countryb == countryb) & (data.year == year)
                condition_b = (data.countrya == countryb)&(data.countryb == countrya) & (data.year == year)
                
                #check if both values are null (if true continue)
                if (data.loc[condition_a, var].isnull().any()== True) & (data.loc[condition_a, var].isnull().any() == True):
                    continue
                else:
                    #set value for reverse pair
                    data.loc[condition_b, var] = data.loc[condition_a, var].values[0]

            used.append(pair)
#__________________________________________
#results
time_taken = round(time.time() - start_time, 2)
print(time_taken)
print('____________________')
