
# coding: utf-8

# In[24]:


import tables
import pandas as pd
import csv
import copy
import datetime as dt

#Read cleaned file
raw_mindright_df = pd.read_csv(r"C:\Users\Rasiga\Documents\MindRight\mindright-csv-export-staging-2018-05-08.csv")

#Example values - arguments to the function
#sdate = '2018-01-03'
#edate = '2018-04-07'
#student_id = '597a2c552ff61c0011f3ec8e'
#timeseries_unit = 'daily'

# sdate = Start of time period
# edate = End of time period
# student_id = id of the student for whom the specific metric is calculated
# timeseries_unit = {daily, hourly, weekly}

def avg_student_response_rate(sdate, edate, student_id, timeseries_unit):
    # deep copy raw DF for processing
    mid_mindright_df = copy.deepcopy(raw_mindright_df)

    # replace [spaces] with '_'
    mid_mindright_df.columns = mid_mindright_df.columns.str.replace(' ','_')

    # lowercase colnames
    mid_mindright_df.columns = mid_mindright_df.columns.str.lower()

    #create timestamp column via concatenating date and time columns
    mid_mindright_df['timestamp'] = pd.to_datetime(mid_mindright_df["date"] + " " + mid_mindright_df["time"], format = "%m/%d/%Y %H:%M")

    # cast date column from string to date type
    mid_mindright_df['date'] = pd.to_datetime(mid_mindright_df['date'], format = "%m/%d/%Y")

    # replace empty messages (images) with '[image]'    
    mid_mindright_df['text_message'] = mid_mindright_df['text_message'].fillna('[image]')

    # subset dataframe to desired date ranges: rename to mindright_df
    mindright_df = mid_mindright_df[(mid_mindright_df.date >= sdate) & (mid_mindright_df.date <= edate)]

    # if timeseries_unit is 'hourly', set date column to timestamp rounded hourly else leave as date
    if timeseries_unit == 'hourly':
        mindright_df['date'] = mindright_df['timestamp'].dt.round('H')

    # if timeseries_unit is 'weekly', set date column to first Monday of the week before the date
    elif timeseries_unit == 'weekly':
        mindright_df['date'] = mindright_df['date'].dt.to_period('W').apply(lambda r: r.start_time)

    # filter for just rows for this student_id
    mindright_df = mindright_df[mindright_df['student_id'] == student_id]

    # Aggregate no.of messages at the(date, direction) level
    date_student_direct_groups = mindright_df.groupby(['date','direction'])['text_message'].agg(['count'])

    #pivot by direction 
    date_student_direct_groups = date_student_direct_groups.unstack(level = -1)

    # converting the groupBy object into a Dataframe and removing null values
    df_date_student_direct_groups = date_student_direct_groups.reset_index().fillna(0) 

    coach_sent_and_student_responded = len(df_date_student_direct_groups[(df_date_student_direct_groups['count']['sent'] > 0) & (df_date_student_direct_groups['count']['received'] > 0)])
    coach_sent = len(df_date_student_direct_groups[df_date_student_direct_groups['count']['sent'] > 0])
    return coach_sent_and_student_responded*100.00/coach_sent


# In[25]:


avg_student_response_rate('2018-01-03', '2018-05-07', '597a2c552ff61c0011f3ec8e', 'weekly')

