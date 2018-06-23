#number of conversations over specified period of time
def num_convo_count(sdate, edate, hourly):
    
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
    if hourly:
        mindright_df['date'] = mindright_df['timestamp'].dt.round('H')

    # count number of unique students by date and text direction
    output = mindright_df.groupby(['date', 'direction'])[['student_id']].nunique().reset_index()

    # if hourly, create a continuous hourly time-series and left-join output to time-series
    if hourly:
        ranges = pd.Series(pd.date_range(start = sdate, end = edate, freq='H'))
        date = pd.concat([ranges, ranges])
        direction = np.repeat(['received','sent'], date.shape[0]/2)
        daypart_range = pd.DataFrame({'date' : date, 'direction' : direction})

        return pd.merge(daypart_range, output,  how='left', left_on=['date','direction'], right_on = ['date','direction']).fillna(0)
    else:
        return output