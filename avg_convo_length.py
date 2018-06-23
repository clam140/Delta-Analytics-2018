# avg, 25 percentile, 75 percentile, min, and max of conversation length over specified time period
def avg_convo_length(sdate, edate, hourly):
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

    # if 'hourly' argument is true, set date column to timestamp rounded hourly else leave as date
    if hourly:                                          #REQUIRED
        mindright_df['date'] = mindright_df['timestamp'].dt.round('H')

    # count total text messages by date and student_id and then get statistics (mean, min, max, etc.) for number of text messages by date
    by_student = mindright_df.groupby(['date','student_id'])['text_message'].count().reset_index()
    output = by_student.groupby(['date'])['text_message'].describe().reset_index()

    # if hourly, create a continuous hourly time-series and left-join output to time-series, then fill NA as 0 to signify no activity
    if hourly:
        date = pd.Series(pd.date_range(start = sdate, end = edate, freq='H'))
        daypart_range = pd.DataFrame({'date' : date})
        output = pd.merge(daypart_range, output,  how='left', left_on='date', right_on = 'date').fillna(0)

    return output