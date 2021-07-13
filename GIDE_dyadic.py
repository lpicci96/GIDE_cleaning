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
nulls_to_skip = pd.DataFrame()

def replace_nulls_in_data(nulls_to_clean, variable, thread_number):
    global data
    global nulls_to_skip
    print(f"Thread {thread_number} is working on replacing nulls {nulls_to_clean[0]} to {nulls_to_clean[len(nulls_to_clean) - 1]}")
    for null in nulls_to_clean:
        if null in nulls_to_skip['row_index']:
            continue
        else:
            try:
                condition = ((data.countryb == data.loc[null, :][0])& 
                        (data.countrya == data.loc[null, :][1]) & 
                        (data.year == data.loc[null, :][2]))
                if data.loc[condition, variable].isnull().any():  #ignore if mirrored value is also null
                    null_to_skip = pd.DataFrame({"id":data.loc[condition, 'directeddyadid'], "row_index":null})
                    nulls_to_skip = nulls_to_skip.append(null_to_skip)
                else:
                    data.loc[null, variable] = data.loc[condition, variable].values[0] # replace null with mirror value
            except Exception as e:
                print(f"Thread {thread_number} crashed due to {e}")

    print(f"Thread Number {thread_number} is done replacing nulls")
    

def correct_non_null_values(years, variable, thread_number):
    global nulls_to_skip
    for year in years:
        print(f"Thread {thread_number} is working on year {year}")
        countries = data.loc[data.year == year, 'countrya'].unique()
        used = [] #empty list used to store fixed country pairs
        for countrya in countries:
            for countryb in countries:
                pair = [countrya, countryb]
                condition_a = (data.countrya == countrya) & (data.countryb == countryb) & (data.year == year)
                condition_b = (data.countrya == countryb)&(data.countryb == countrya) & (data.year == year)
                id_a = data.loc[condition_a, "directeddyadid"]
                id_b = data.loc[condition_b, "directeddyadid"]
                if len(nulls_to_skip) != 0:
                    if tuple(id_a) in nulls_to_skip['id'] or tuple(id_b) in nulls_to_skip['id']: #If we already know they are both null, skip
                        continue
                elif (countrya == countryb) or (pair in used) or ([countryb, countrya] in used): #If we know this pair has already been cleaned, skip
                    continue
                #else fix values that exist
                else:
                    #set value for reverse pair
                    data.loc[condition_b, variable] = data.loc[condition_a, variable].values[0]

                used.append(pair)
        print(f"Thread {thread_number} is done with year {year}")

#__________  Initialization _____________
if __name__ == "__main__":
    cwd = os.getcwd()
    path = cwd + "\GIDE_1.82_20201002_.csv"
    #var = 'totaltradeawithb'  #set variable name

    data = pd.read_csv(path, low_memory=False) #If you have less than 32GB of ram then set "low_memory" to True!
    variables = ['distanceatob', 'distancecapitalatob', 'distancepopwghtdatob','distancepopwghtdgravityatob','distatob_searoute','sharedborder','colonialitiescow','colonialities1945cepii','commoncolonizer','commonofficiallanguage','commonspokenlanguage','commonnativelanguage','commonlanguageindex','sharedreligionindex','totaltradeawithb'] #Put in Variables as Array of Strings, then run DOUBLE CHECK CAPITALIZATION AND SPELLING ARE CORRECT!

    #Once a Variable is done, while the next is running do a quick review of the output CSV for any obvious mistakes.
    #If it looks clean then you may remove it from the above array so errors don't cause retreading.
    #If there is a clear issue, start searching for what caused it and let Cory know, so we can make sure this isn't systemic to other variables
    for variable in variables:
        start_time = time.time()
        print('---', variable, '---')

        #________________________________________
        ### Fix Null Values
        nulls = data[data[variable].isnull()].index.tolist() # make list of null value index
        if len(nulls) != 0:
            nulls_to_skip["id"] = ""
            nulls_to_skip["row_index"] = ""
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
                    thread = threading.Thread(target= replace_nulls_in_data, args=(thread_nulls, variable, core))
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
                thread_years= years[thread_start : thread_end]
            try:
                thread = threading.Thread(target= correct_non_null_values, args=(thread_years, variable, core))
                threads.append(thread)
            except Exception as e:
                print(f"I was stopped from launching thread {core} because of {e}")
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        #All of the below will allow us to keep progress of variables if a crash happens
        data_out = data[['directeddyadid', variable]]
        data_out.to_csv(f"{cwd}\\cleaned_variables\\cleaned_data_{variable}.csv") #The output CSV will have a primary key and the cleaned variable
        time_taken = round(time.time() - start_time, 2)
        print(time_taken)
        print('____________________')


