
# Input the start and end date, student_id and timeseries_unit to get the no.of messages sent by the student in that time period
# at the weekly/daily/hourly level

#Example values - arguments to the function
#start = '2018-01-03'
#end = '2018-04-07'
#student_id = '597a2c552ff61c0011f3ec8e'

# sdate = Start of time period
# edate = End of time period
# student_id = id of the student for whom the specific metric is calculated
# timeseries_unit = {daily, hourly, weekly}

def num_messages_sent_by_student(sdate, edate, student_id, timeseries_unit):
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
 
    # Get all rows where direction=sent (sent from MR);  and then get the no.of messages sent from MR per student per day
    df_sent = mindright_df[(mindright_df['direction'] == 'sent') & (mindright_df['student_id'] == student_id)]
	
    # group the messages sent to students by date, student_id
    df_sent_grouped_by_date = df_sent.groupby(['date'])
	
    # aggregating the no.of messages the data grouped by date, student
    output_df = df_sent_grouped_by_date.agg('size')
    return output_df
