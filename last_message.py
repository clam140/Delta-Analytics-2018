# last message of specified student
def last_message(s_id):
    # deep copy raw DF for processing
    mid_mindright_df = copy.deepcopy(raw_mindright_df)
    # replace [spaces] with '_'
    mid_mindright_df.columns = mid_mindright_df.columns.str.replace(' ','_')
    # lowercase colnames
    mid_mindright_df.columns = mid_mindright_df.columns.str.lower()
    #create timestamp column via concatenating date and time columns
    mid_mindright_df['timestamp'] = pd.to_datetime(mid_mindright_df['date'] + ' ' + mid_mindright_df['time'], format = '%m/%d/%Y %H:%M')
    # cast date column from string to date type
    mid_mindright_df['date'] = pd.to_datetime(mid_mindright_df['date'], format = "%m/%d/%Y")
    # replace empty messages (images) with '[image]'    
    mid_mindright_df['text_message'] = mid_mindright_df['text_message'].fillna('[image]')

    subset = mindright_df[(mindright_df['student_id'] == s_id) & (mindright_df['direction'] == 'received')]
    
    last_activity = max(subset['date'])
    
    current_date = dt.datetime.now()
    
    time_gap = current_date - last_activity
    
    return(time_gap)