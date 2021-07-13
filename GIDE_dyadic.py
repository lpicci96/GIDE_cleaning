import numpy as np
import pandas as pd
import math
import time
import os
import threading
import multiprocessing
'''
This script fixes dyadic series in the gide dataset.

- If both mirrored values are null, ignore
- If only one value is null, replace value with the mirrored values (regardless of alphabetical order)
- If values for mirrored pairs differ, take the value in alphabetical 
order of countrya and apply it to the mirrored value.

'''
number_of_cores = multiprocessing.cpu_count()
data: pd.DataFrame
nulls: pd.DataFrame

def replace_nulls_in_data(nulls_to_clean, thread_number):
    global data
    print(f"Thread {thread_number} is working on replacing nulls {nulls_to_clean[0]} to {nulls_to_clean[len(nulls_to_clean) - 1]}")
    for null in nulls_to_clean:
        try:
            condition = ((data.countryb == data.loc[null, :][0])& 
                    (data.countrya == data.loc[null, :][1]) & 
                    (data.year == data.loc[null, :][2]))
            if data.loc[condition, var].isnull().any():  #ignore if mirrored value is also null
                continue
            else:
                data.loc[null, var] = data.loc[condition, var].values[0] # replace null with mirror value
        except Exception as e:
            print(f"Thread {thread_number} crashed due to {e}")

    print(f"Thread Number {thread_number} is done replacing nulls")
    

def correct_non_null_values(years, thread_number):
    for year in years:
        print(f"Thread {thread_number} is working on year {year}")
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
            print(f"Thread {thread_number} is done with year {year}")

#__________  Initialization _____________
if __name__ == "__main__":
    cwd = os.getcwd()
    path = cwd + "\GIDE_1.82_20201002_.csv"
    var = 'totaltradeawithb'  #set variable name

    #read file


    data = pd.read_csv(path)
    start_time = time.time()
    print('---', var, '---')

    #________________________________________
    ### Fix Null Values
    nulls = data[data[var].isnull()].index.tolist() # make list of null value index
    nulls_per_thread = len(nulls) / number_of_cores
    nulls_per_thread = round(nulls_per_thread)
    thread_nulls = list
    threads = []
    for core in range(0, number_of_cores):
        if core != number_of_cores - 1:
            thread_start = (nulls_per_thread * core)
            thread_end = (nulls_per_thread * (core + 1))
            thread_nulls = nulls[thread_start : thread_end]
        else:
            thread_start = (nulls_per_thread * core)
            thread_end = len(nulls)
            thread_nulls= nulls[thread_start : thread_end]
        try:
            thread = threading.Thread(target= replace_nulls_in_data, args=(thread_nulls, core))
            threads.append(thread)
        except Exception as e:
            print(f"I was stopped from launching thread {core} because of {e}")
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    years = data.year.unique()
    year_range = len(years)
    years_per_thread = year_range / number_of_cores
    years_per_thread = round(years_per_thread)
    threads = []
    for core in range(0, number_of_cores):
        if core != number_of_cores - 1:
            thread_start = (years_per_thread * core)
            thread_end = (years_per_thread * (core + 1))
            thread_years = years[thread_start : thread_end]
        else:
            thread_start = (years_per_thread * core)
            thread_end = len(years)
            thread_years= nulls[thread_start : thread_end]
        try:
            thread = threading.Thread(target= correct_non_null_values, args=(thread_years, core))
            threads.append(thread)
        except Exception as e:
            print(f"I was stopped from launching thread {core} because of {e}")
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    time_taken = round(time.time() - start_time, 2)
    print(time_taken)
    print('____________________')


