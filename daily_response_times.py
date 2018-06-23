#average daily response times by day of week and hour
def daily_response_times(sdate, edate):
    # deep copy raw DF for processing
    mid_mindright_df = copy.deepcopy(raw_mindright_df)
    # replace [spaces] with '_'
    mid_mindright_df.columns = mid_mindright_df.columns.str.replace(' ','_')
    # lowercase colnames
    mid_mindright_df.columns = mid_mindright_df.columns.str.lower()
        # cast date column from string to date type
    mid_mindright_df['date'] = pd.to_datetime(mid_mindright_df['date'], format = "%m/%d/%Y")
    # subset
    mindright_df = mid_mindright_df[(mid_mindright_df.date >= sdate) & (mid_mindright_df.date <= edate) & (mid_mindright_df.direction == 'received')]

    time_obj = mindright_df['time'].apply(lambda x: dt.datetime.strptime(x, '%H:%M').time())

    mindright_df['hour'] = time_obj.apply(lambda i: i.hour)
    mindright_df['day_of_week'] = mindright_df['date'].apply(lambda x: x.strftime("%A"))
    output = mindright_df.groupby(['day_of_week','hour'])['student_id'].apply(lambda x: np.mean(x.nunique())).reset_index()
    return(output)