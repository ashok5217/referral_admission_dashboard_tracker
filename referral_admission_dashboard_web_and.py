


from Ashok import *


# In[51]:


application=pd.read_sql("select a.profile_slug,a.status,a.funnel_entry_date,a.created_at,a.launch_form,a.onboarding_session_one_time_link , b.name,b.email,a.masai_foundation_program,batch.application_start_date,batch.start_date as batch_start_date from application a  left join profile b on a.profile_slug=b.slug  left join batch_campus on a.batch_campus_id = batch_campus.id left join batch on batch_campus.batch_id = batch.id where  batch_campus.batch_id = 11 and batch_campus.course_specialization_id = 1 and a.status not in ('INELIGIBLE','PROFILE_UNFILLED') and b.referred_by is not null",con)


# In[52]:


application['Registration'] = True


# In[53]:


application


# In[54]:


application['masai_foundation_program'] = application['masai_foundation_program'].fillna('No')
application['masai_foundation_program'] = application['masai_foundation_program'].replace((0.0, 1.0),("No","Yes"))


# In[55]:


application['email'].value_counts()[application['email'].value_counts()>1]


# In[56]:


application['batch_start_date'] = application['batch_start_date'].dt.date


# In[57]:


application['masai_foundation_program'].unique()
application = application[application['masai_foundation_program']=='No']


# In[58]:


application


# In[59]:


application=application[application.groupby('email')['funnel_entry_date'].transform(max)==application['funnel_entry_date']]


# In[60]:


application['status'].value_counts()


# In[61]:


def get_states(row):
    msat_attempted_list=[ 'METTL_STARTED','METTL_DESCISION_PENDING', 'METTL_FAILED','METTL_PASSED','ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW', 'APPLICATION_CLOSED']
    msat_cleared_list=['METTL_PASSED','ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW', 'APPLICATION_CLOSED']
    verification_pending_list=['METTL_PASSED']
    verification_complete_list=['ONBOARDING_PENDING','ONBOARDING_STARTED','ONBOARDING_COMPLETE','FELLOW']
    self_onboarding_pending_list=['ONBOARDING_PENDING','ONBOARDING_STARTED'] 
    self_onboarding_completed_list=['ONBOARDING_COMPLETE','FELLOW']
    self_onboarding_pending=['ONBOARDING_PENDING','ONBOARDING_STARTED']
    #assisted_onboarding_booked_list='onboarding_session_one_time_link' is not null
    assisted_onboarding_pending_list=['ONBOARDING_PENDING','ONBOARDING_STARTED']
    assisted_onboarding_completed_list=['ONBOARDING_COMPLETE','FELLOW']
    onboarding_pending_list=['ONBOARDING_PENDING','ONBOARDING_STARTED']
    application_closed = ['APPLICATION_CLOSED']
    mettl_decision_pending = ['METTL_DESCISION_PENDING']
    
    #launch_form_filled_list='launch_form' not null
    #Webinar Joined
    #Telegram Channel Joined
    pre_course_done_list=['ONBOARDING_COMPLETE']
    try:
        msat_attempted=row['status']in msat_attempted_list
    except:
        msat_attempted=False
        
        
        
    try:
        msat_cleared=row['status'] in msat_cleared_list
    except:
        msat_cleared=False
        
        
        
    try:
        verification_pending=row['status'] in verification_pending_list
    except:
        verification_pending=False
        
        
        
    try:
        verification_complete=row['status'] in verification_complete_list
    except:
        verification_complete=False
        
        
    try:
        self_onboarding_pending=(row['status'] in self_onboarding_pending_list) & (pd.isna(row['onboarding_session_one_time_link']))
    except:
        self_onboarding_pending=False
    
    
    try:
        self_onboarding_completed=(row['status'] in self_onboarding_completed_list) & (pd.isna(row['onboarding_session_one_time_link']))
    except:
        self_onboarding_completed=False
        
    try:
        self_onboarding_completed=(row['status'] in self_onboarding_completed_list) & (pd.isna(row['onboarding_session_one_time_link']))
    except:
        self_onboarding_completed=False

        
        
    try:
        assisted_onboarding_booked=pd.isna(row['onboarding_session_one_time_link'])==False
    except:
        assisted_onboarding_booked=False
        
        
    try:
        assisted_onboarding_pending=(row['status'].isin(assisted_onboarding_pending_list)) & (pd.isna(row['onboarding_session_one_time_link'])==False)
    except:
        assisted_onboarding_pending=False
        
        
        
    try:
        assisted_onboarding_completed=(row['status'].isin(assisted_onboarding_completed_list)) & (pd.isna(row['onboarding_session_one_time_link'])==False)
    except:
        assisted_onboarding_completed=False
        
        
    try:
        onboarding_pending=row['status'].isin(onboarding_pending_list)
    except:
        onboarding_pending=False
        
    try:
        mettl_decision_pending=row['status'].isin(mettl_decision_pending)
    except:
        mettl_decision_pending=False
        
    try:
        application_closed=row['status'].isin(application_cllosed)
    except:
        application_closed=False
    
        
    try:
        launch_form_filled=pd.isna(row['launch_form'])==False
    except:
        launch_form_filled=False
    
    #try:
        #pre_course_done=row['status'].isin(pre_course_done_list)
    #except:
        #pre_course_done=False
    #Webinar Joined,#Telegram Channel Joined
    return pd.Series([msat_attempted, msat_cleared, verification_pending,verification_complete,self_onboarding_pending,self_onboarding_completed,assisted_onboarding_booked,assisted_onboarding_pending,onboarding_pending ,launch_form_filled])




# In[62]:


application[['msat_attempted', 'msat_cleared', 'verification_pending','verification_complete','self_onboarding_pending','self_onboarding_completed','assisted_onboarding_booked','assisted_onboarding_pending','onboarding_pending' ,'launch_form_filled']]=application.apply(lambda row:get_states(row),axis=1)


# In[63]:


application['status'].value_counts().sum()


# In[64]:


application['mettl_decision_pending'] = application['status'] == 'METTL_DESCISION_PENDING'
application['application_closed'] = application['status']== 'APPLICATION_CLOSED'
application['pre_course_done'] = application['status']== 'ONBOARDING_COMPLETE'

application['onboarding_started'] = application['status']== 'ONBOARDING_STARTED'
application['assisted_onboarding_pending'] = (application['status'].isin(['ONBOARDING_PENDING','ONBOARDING_STARTED'])) & (pd.isna(application['onboarding_session_one_time_link'])==False)
application['assisted_onboarding_complete'] = (application['status'].isin(['ONBOARDING_COMPLETE','FELLOW'])) & (pd.isna(application['onboarding_session_one_time_link'])==False)


# In[65]:


#application['day'] = pd.to_datetime('today').date()-application['funnel_entry_date'].dt.date


# In[66]:


application.head(2)


# In[67]:


application['day'] = (pd.to_datetime('today').date()-application['created_at'].dt.date).dt.days
application['wee']=(application['batch_start_date']-(application['created_at'].dt.date)).dt.days
#application['day'] = application[application['day']<=6]


# In[68]:


def week(x):
    if x<14:
        return 4
    if x<28:
        return 3
    if x<42:
        return 2
    if x<56:
        return 1
    else:
        return 0


# In[69]:


pd.DataFrame(application['wee'].value_counts()).sort_index()


# In[ ]:





# In[70]:



#application['day']=(pd.to_datetime('today').date()-application['created_at'].dt.date).dt.days


# In[71]:


(pd.to_datetime('today')+pd.Timedelta(minutes = 330)).date()


# In[72]:


#application['day']=(application['created_at'].dt.date-pd.to_datetime('today').date())
#application['week']=(((application['created_at'].dt.date-application['application_start_date'].iloc[1].date()).dt.days)/7).astype(int)
#application.loc[application['week']<0,'week']=0


# In[73]:


application['week'] = application['wee'].apply(week)


# In[74]:


application['total_registration']=True


# In[75]:


daily_applications=application[application['day']<=6].copy()


# In[76]:


application.groupby('week')['wee'].value_counts()


# In[77]:


#daily_applications['day']="D-"+daily_applications['day'].astype(int).astype(str)


# In[78]:


df1 = daily_applications.groupby('day').agg({'total_registration':sum,'msat_attempted':sum,'msat_cleared':sum,'verification_pending':sum,'verification_complete':sum,'verification_complete':sum,'self_onboarding_completed':sum,'self_onboarding_pending':sum,'assisted_onboarding_booked':sum,'assisted_onboarding_pending':sum,'assisted_onboarding_complete':sum,'launch_form_filled':sum,'pre_course_done':sum,'mettl_decision_pending':sum,'application_closed':sum,'pre_course_done':sum,'onboarding_started':sum})


# In[ ]:





# In[79]:


df1=df1.reindex(pd.Index(range(7),fill_value = 0)).fillna(0).astype(int)


# In[80]:


df1 = df1.transpose()


# In[81]:


df1


# In[82]:


df2  = application.groupby('week').agg({'total_registration':sum,'msat_attempted':sum,'msat_cleared':sum,'verification_pending':sum,'verification_complete':sum,'verification_complete':sum,'self_onboarding_completed':sum,'assisted_onboarding_booked':sum,'assisted_onboarding_pending':sum,'assisted_onboarding_complete':sum,'launch_form_filled':sum,'pre_course_done':sum,'mettl_decision_pending':sum,'application_closed':sum,'pre_course_done':sum,'onboarding_started':sum})


# In[83]:


df2.index = df2.index.astype(int)


# In[84]:


application['week'].value_counts()


# In[85]:


new_ix = pd.Index(range(application['week'].max()+1))
#output = df2.set_index(['user_id', 'quiz_id'])
df2=df2.reindex(new_ix, fill_value=0).sort_index().transpose()


# In[86]:


#df2=df2.sort_index().transpose()


# In[87]:


df2




df1.columns=["D-{}".format(x) for x in df1.columns]
df2.columns=["Week {}".format(x) for x in df2.columns]


# In[90]:


df1


# In[91]:


old_cols=df2.columns


# In[92]:


df2['cumulative']=df2.sum(axis=1)


# In[93]:


df2=df2[['cumulative']+list(old_cols)]


# In[94]:


df2


# In[95]:


df2=df1.merge(df2,how='outer',left_index=True,
    right_index=True)
#populate("https://docs.google.com/spreadsheets/d/1X4PPWJWRjHKZAigN9RUsZH8fY6lHb3LBM3h-4xr6bb0/edit#gid=222379688","pt_raw",df2,row=3,col=3)


# In[96]:


df2 = df2.reindex(['total_registration','msat_attempted','mettl_decision_pending','msat_cleared','verification_pending','verification_complete','self_onboarding_completed','self_onboarding_pending','assisted_onboarding_complete','assisted_onboarding_booked','assisted_onboarding_pending','launch_form_filled','pre_course_done','application_closed','onboarding_started']).fillna(0).astype(int)


# In[97]:


df2


# In[98]:


#populate("https://docs.google.com/spreadsheets/d/1X4PPWJWRjHKZAigN9RUsZH8fY6lHb3LBM3h-4xr6bb0/edit#gid=1123495114","da_pt_raw",df2,row = 2,col = 2)


# In[ ]:


populate("https://docs.google.com/spreadsheets/d/14m4ntZrZ_ABpxLQh-fJBhkVUECWeayzWsUhA2AzTN48/edit#gid=1122234254","pt_raw",df2,row = 22,col = 2)









#Journey date dashboard
from Ashok import *
#!/usr/bin/env python
# coding: utf-8

# In[1]:


df = pd.read_sql("""select p.name,p.email,p.created_at profile_created_at,a.onboarding_session_one_time_link, a.slug,a.masai_foundation_program,a.onboarding_session_one_time_link,  j.new_status,j.application_slug,j.created_at as journey_date,b.batch_id,b.course_specialization_id,batch.start_date as batch_start_date,onboarding_session.start_date_time, JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason,batch.start_date from application a  left join  funnel_entry_date j  on a.slug = j.application_slug left join profile as p  on  a.profile_slug = p.slug left join batch_campus b on a.batch_campus_id = b.id left join batch on batch.id = b.batch_id  left join onboarding_session on onboarding_session.application_slug = a.slug  where b.batch_id = 11 and b.course_specialization_id = 1 and a.status not in ("INELIGIBLE","PROFILE_UNFILLED") and p.referred_by is not null""",con)


# In[41]:


df.head(2)


# In[42]:


df['journey_date'] = (df['journey_date']+pd.Timedelta(minutes = 330)).dt.date
df['journey_date'] = pd.to_datetime(df['journey_date'])
#df = df[df['masai_foundation_program']==1.00]

df['day'] = ((pd.to_datetime("today")-pd.to_datetime(df['journey_date'])).dt.days)
df.dropna(subset = ['day'],inplace = True)


# In[43]:


df['wee']=(df['batch_start_date'].dt.date-(df['journey_date'].dt.date)).dt.days


# In[44]:


def week(x):
    if x<14:
        return 4
    if x<28:
        return 3
    if x<42:
        return 2
    if x<56:
        return 1
    else:
        return 0


# In[45]:


#df['week'] = (((pd.to_datetime("today")-pd.to_datetime(df['journey_date'])).dt.days)/7).astype(int)

df.rename(columns = {'new_status':'status','start_date_time':'onboarding_start_time'},inplace = True)


# In[46]:


def week(x):
    if x<14:
        return 4
    if x<28:
        return 3
    if x<42:
        return 2
    if x<56:
        return 1
    else:
        return 0


# In[47]:


df['week'] = df['wee'].apply(week)


# In[48]:


list_x = list(df[df['status']=='ONBOARDING_PENDING']['email'])
list_y = list(df[df['status']=='APPLICATION_CLOSED']['email'])
set1 = set(list_x)
set2 = set(list_y)
intersect = set1.intersection(set2)
result = list(intersect)
result


# In[49]:





df['Graduation'] = df['reject_reason'].isin(['To be eligible for this course you must be graduating in 2022','To be eligible for this course you must be graduating before 2022'])
df['Age'] = df['reject_reason']=='You must be between 18 and 28 years old for Software Development courses'
df['Not ready for job after graduation'] = df['reject_reason'] == 'You must be ready to take a job after graduation'
#df['Unsucessfull Application'] = df['Ineligible']-df['Graduation'] -df['Age']-df['Not ready for job after graduation']

df['Eligible Application'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED','FELLOW'])
df['Profile Unfilled'] = df['status']=='PROFILE_UNFILLED'
#df['Eligible & Profile Unfilled'] = df['status'].isin(['METTL_TO_BE_TAKEN',  'METTL_FAILED','ONBOARDING_COMPLETE', 'ONBOARDING_PENDING','METTL_DESCISION_PENDING', 'ONBOARDING_STARTED', 'METTL_PASSED','APPLICATION_CLOSED', 'METTL_STARTED'])
df['Mettl to be taken'] = df['status']== 'METTL_TO_BE_TAKEN'
df['mettl_attempted']=df['status'].isin([ 'METTL_STARTED'])
df['mettl_cleared']=df['status'].isin(['METTL_PASSED'])
df['verification_pending']=df['status']=='METTL_PASSED'

df['Mettl Started'] = df['status']== 'METTL_STARTED'
df['Mettl Failed'] = df['status']== 'METTL_FAILED'
df['Mettl Passed'] = df['status']== 'METTL_PASSED'
df['Application Closed'] = df['status']== 'APPLICATION_CLOSED'
df['Mettl Decesion Pending'] = df['status']== 'METTL_DESCISION_PENDING'
df['onboarding pending']=df['status']=='ONBOARDING_PENDING'
df['onboarding complete']=df['status']=='ONBOARDING_COMPLETE'
df['onboarding started']=df['status']=='ONBOARDING_STARTED'
df['verification_complete']=df['status']=='ONBOARDING_PENDING'
df['Assisted onboarding completed']=(df['status'].isin(['ONBOARDING_COMPLETE'])) & (pd.isna(df['onboarding_start_time'])==False)
df['Self onboarding completed']=(df['status'].isin(['ONBOARDING_COMPLETE'])) & (pd.isna(df['onboarding_start_time']))
df['Assisted onboarding started']=(df['status'].isin(['ONBOARDING_STARTED'])) & (pd.isna(df['onboarding_start_time'])==False)
df['Self onboarding started']=(df['status'].isin(['ONBOARDING_STARTED'])) & (pd.isna(df['onboarding_start_time']))


# In[ ]:





# In[ ]:





# In[50]:


df = df.drop_duplicates(['email','status'])


# In[51]:


df['journey_date'] = pd.to_datetime(df['journey_date'])


# In[52]:


df.loc[(df['email'].isin(result)) & (df['status']=="ONBOARDING_PENDING"),'status']=None


# In[53]:


df['verification_complete']=df['status']=='ONBOARDING_PENDING'


# In[54]:


#df = df[df['journey_date']<'2022-02-27']


# In[55]:


df_d = df.groupby('day').agg({'Graduation':sum, 'Age':sum,
           'Not ready for job after graduation':sum, 'Eligible Application':sum,
           'Profile Unfilled':sum, 'Mettl to be taken':sum, 'mettl_attempted':sum,
           'mettl_cleared':sum, 'verification_pending':sum, 'verification_complete':sum,
           'Mettl Decesion Pending':sum, 'Mettl Started':sum, 'Mettl Failed':sum, 'Mettl Passed':sum,'onboarding pending':sum,
            'onboarding complete':sum,'onboarding started':sum,'Self onboarding completed':sum,
            'Self onboarding completed':sum,'Self onboarding completed':sum,'Assisted onboarding completed':sum, 
            'Assisted onboarding started':sum,'Self onboarding started':sum,'Application Closed':sum}).transpose().fillna(0).astype(int)


# In[56]:


df_d


# In[57]:


df_d = df_d.transpose()


    #


    # In[32]:


# In[58]:


ind = pd.Index(range(7))
ind


# In[59]:


df_d = df_d.reindex(ind,fill_value = 0)


    # In[33]:


df_d


    # In[34]:



# In[60]:


df_d = df_d.transpose()


# In[61]:



df_d = df_d.transpose()


    # In[36]:



# In[62]:


df_d = df_d[df_d.index<7]


    # In[37]:


df_d = df_d.transpose()


# In[63]:





    # In[38]:


df_d.columns = ["Day{}".format(i) for i in df_d.columns]


# In[48]:


df_w = df.groupby('week').agg({'Graduation':sum, 'Age':sum,
           'Not ready for job after graduation':sum, 'Eligible Application':sum,
           'Profile Unfilled':sum, 'Mettl to be taken':sum, 'mettl_attempted':sum,
           'mettl_cleared':sum, 'verification_pending':sum, 'verification_complete':sum,
           'Mettl Decesion Pending':sum, 'Mettl Started':sum, 'Mettl Failed':sum, 'Mettl Passed':sum,
            'onboarding pending':sum,'onboarding complete':sum,'onboarding started':sum,'Self onboarding completed':sum,
            'Assisted onboarding completed':sum,
            'Assisted onboarding started':sum,'Self onboarding started':sum,'Application Closed':sum})


# In[64]:



# In[49]:


df_w = df_w.reindex(range(5),fill_value = 0)


# In[50]:


df_w


# In[51]:


df_w = df_w.transpose()


# In[52]:


df_w


# In[53]:





# In[65]:


df_w.columns = ["Week{}".format(i) for i in df_w.columns]



# In[66]:



# In[54]:


df_w


# In[55]:


old_col = df_w.columns


# In[56]:


df_w['Cumalative'] = df_w.sum(axis = 1)


# In[67]:


df_w


# In[ ]:





# In[68]:



#df_w = df_w[['Cumalative']+list(old_col)]

df_w


# In[69]:



    # In[47]:


final = pd.merge(df_d,df_w,left_index = True,right_index = True,how = 'outer')


    # In[48]:



    


# In[70]:


final


# In[72]:


final.index = [i.title() for i in final.index]
final.index = [i.replace("_"," ") for i in final.index]


# In[73]:


final


# In[74]:


list_x = list(df[df['status']=='ONBOARDING_PENDING']['email'])
list_y = list(df[df['status']=='APPLICATION_CLOSED']['email'])

set1 = set(list_x)
set2 = set(list_y)

intersect = set1.intersection(set2)
result = list(intersect)
result


# In[75]:


final.index


# In[77]:


final = final.loc[['Mettl Attempted','Mettl Cleared','Verification Complete','Profile Unfilled']]


# In[78]:


final


# In[64]:


final = final.iloc[:,[0,1,2,3,4,5,6,12,7,8,9,10,11]]





# In[148]:


#populate("https://docs.google.com/spreadsheets/d/1uVGZsrQAwZcuhRMKUjXqn0mKMd1qVBWw2cdKb75OEZA/edit#gid=644836134","Sheet7",df[df['verification_complete']==True],row = 1,col = 1)


final = final.loc[['Mettl Attempted','Mettl Cleared','Verification Complete','Profile Unfilled']]


# In[39]:


#populate("https://docs.google.com/spreadsheets/d/1uVGZsrQAwZcuhRMKUjXqn0mKMd1qVBWw2cdKb75OEZA/edit#gid=644836134","Sheet7",df[df['verification_complete']==True],row = 1,col = 1)


# In[79]:






# In[88]:


final
populate("https://docs.google.com/spreadsheets/d/14m4ntZrZ_ABpxLQh-fJBhkVUECWeayzWsUhA2AzTN48/edit#gid=1122234254","pt_raw",final.astype(str),row = 44,col = 2)




