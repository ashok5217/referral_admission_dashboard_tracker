#!/usr/bin/env python
# coding: utf-8

# In[2]:


from Ashok import *


# In[5]:


ws = client.open_by_url("https://docs.google.com/spreadsheets/d/1Ud5iwiH7961bYntTXHgVWgx6IzuJTabKpHFXNe7d9UY/edit#gid=1142814586").worksheet("raw_data")


# In[6]:


df = pd.DataFrame(ws.get_all_values()[1:],columns = ws.get_all_values()[0])


# In[7]:


def num(x):
    try:
        return int(x)
    except:
        return None


# In[8]:


df['Batch Id'] = df['Batch Id'].apply(num)


# In[9]:


mail_id = tuple(df[df['Batch Id']==7]['Email'].unique())


# In[ ]:





# In[10]:


df = pd.read_sql("""select p.name,p.email,p.created_at profile_created_at,a.onboarding_session_one_time_link, a.slug,a.masai_foundation_program,a.onboarding_session_one_time_link,  j.new_status,j.application_slug,j.created_at as journey_date,b.batch_id,b.course_specialization_id,batch.start_date as batch_start_date,onboarding_session.start_date_time, JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason,batch.start_date from application a  left join  funnel_entry_date j  on a.slug = j.application_slug left join profile as p  on  a.profile_slug = p.slug left join batch_campus b on a.batch_campus_id = b.id left join batch on batch.id = b.batch_id  left join onboarding_session on onboarding_session.application_slug = a.slug  where b.batch_id = 7 and b.course_specialization_id = 1 and a.status not in ("INELIGIBLE","PROFILE_UNFILLED") and p.email in {} """.format(mail_id),con)


# In[41]:


df.head(2)


# In[42]:


df['journey_date'] = (df['journey_date']+pd.Timedelta(minutes = 330)).dt.date
df['journey_date'] = pd.to_datetime(df['journey_date'])
df = df[df['masai_foundation_program']!=1.00]

df['day'] = ((pd.to_datetime("today")-pd.to_datetime(df['journey_date'])).dt.days)
df.dropna(subset = ['day'],inplace = True)


# In[43]:


df['wee']=(df['batch_start_date'].dt.date-(df['journey_date'].dt.date)).dt.days


# In[44]:


def week(x):
    if x<7:
        return 4
    if x<14:
        return 3
    if x<21:
        return 2
    if x<28:
        return 1
    else:
        return 0


# In[45]:


#df['week'] = (((pd.to_datetime("today")-pd.to_datetime(df['journey_date'])).dt.days)/7).astype(int)

df.rename(columns = {'new_status':'status','start_date_time':'onboarding_start_time'},inplace = True)


# In[46]:


def week(x):
    if x<7:
        return 4
    if x<14:
        return 3
    if x<21:
        return 2
    if x<28:
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

df_w = df.groupby('week').agg({'Graduation':sum, 'Age':sum,
           'Not ready for job after graduation':sum, 'Eligible Application':sum,
           'Profile Unfilled':sum, 'Mettl to be taken':sum, 'mettl_attempted':sum,
           'mettl_cleared':sum, 'verification_pending':sum, 'verification_complete':sum,
           'Mettl Decesion Pending':sum, 'Mettl Started':sum, 'Mettl Failed':sum, 'Mettl Passed':sum,
            'onboarding pending':sum,'onboarding complete':sum,'onboarding started':sum,'Self onboarding completed':sum,
            'Assisted onboarding completed':sum,
            'Assisted onboarding started':sum,'Self onboarding started':sum,'Application Closed':sum})


# In[64]:


df_w = df_w.transpose()


# In[65]:


df_w.columns = ["Week{}".format(i) for i in df_w.columns]



# In[66]:


old_col = df_w.columns


    # In[44]:


df_w['Cumalative'] = df.sum(axis = 0)


# In[67]:


df_w


# In[ ]:





# In[68]:



df_w = df_w[['Cumalative']+list(old_col)]

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


final = final.loc[['Mettl Attempted','Mettl Cleared','Verification Complete']]


# In[78]:


final


# In[11]:


populate("https://docs.google.com/spreadsheets/d/1X4PPWJWRjHKZAigN9RUsZH8fY6lHb3LBM3h-4xr6bb0/edit#gid=1925868699","referral_student_dashbaord",final.astype(int),row = 5,col = 5)


# In[12]:


df = pd.read_sql("""select p.name,p.email,p.created_at profile_created_at,a.onboarding_session_one_time_link, a.slug,a.masai_foundation_program,a.onboarding_session_one_time_link,  j.new_status,j.application_slug,j.created_at as journey_date,b.batch_id,b.course_specialization_id,batch.start_date as batch_start_date,onboarding_session.start_date_time, JSON_UNQUOTE(JSON_EXTRACT(a.reject_reason, '$."0"')) as reject_reason,batch.start_date from application a  left join  funnel_entry_date j  on a.slug = j.application_slug left join profile as p  on  a.profile_slug = p.slug left join batch_campus b on a.batch_campus_id = b.id left join batch on batch.id = b.batch_id  left join onboarding_session on onboarding_session.application_slug = a.slug  where b.batch_id = 7 and b.course_specialization_id = 1 and a.status not in ("INELIGIBLE","PROFILE_UNFILLED") and p.email in {} """.format(mail_id),con)


# In[41]:


df.head(2)


# In[42]:


df['journey_date'] = (df['journey_date']+pd.Timedelta(minutes = 330)).dt.date
df['journey_date'] = pd.to_datetime(df['journey_date'])
df = df[df['masai_foundation_program']==1.00]

df['day'] = ((pd.to_datetime("today")-pd.to_datetime(df['journey_date'])).dt.days)
df.dropna(subset = ['day'],inplace = True)


# In[43]:


df['wee']=(df['batch_start_date'].dt.date-(df['journey_date'].dt.date)).dt.days


# In[44]:


def week(x):
    if x<7:
        return 4
    if x<14:
        return 3
    if x<21:
        return 2
    if x<28:
        return 1
    else:
        return 0


# In[45]:


#df['week'] = (((pd.to_datetime("today")-pd.to_datetime(df['journey_date'])).dt.days)/7).astype(int)

df.rename(columns = {'new_status':'status','start_date_time':'onboarding_start_time'},inplace = True)


# In[46]:


def week(x):
    if x<7:
        return 4
    if x<14:
        return 3
    if x<21:
        return 2
    if x<28:
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

df_w = df.groupby('week').agg({'Graduation':sum, 'Age':sum,
           'Not ready for job after graduation':sum, 'Eligible Application':sum,
           'Profile Unfilled':sum, 'Mettl to be taken':sum, 'mettl_attempted':sum,
           'mettl_cleared':sum, 'verification_pending':sum, 'verification_complete':sum,
           'Mettl Decesion Pending':sum, 'Mettl Started':sum, 'Mettl Failed':sum, 'Mettl Passed':sum,
            'onboarding pending':sum,'onboarding complete':sum,'onboarding started':sum,'Self onboarding completed':sum,
            'Assisted onboarding completed':sum,
            'Assisted onboarding started':sum,'Self onboarding started':sum,'Application Closed':sum})


# In[64]:


df_w = df_w.transpose()


# In[65]:


df_w.columns = ["Week{}".format(i) for i in df_w.columns]



# In[66]:


old_col = df_w.columns


    # In[44]:


df_w['Cumalative'] = df.sum(axis = 0)


# In[67]:


df_w


# In[ ]:





# In[68]:



df_w = df_w[['Cumalative']+list(old_col)]

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


final = final.loc[['Mettl Attempted','Mettl Cleared','Verification Complete']]


# In[78]:


final


# In[13]:


populate("https://docs.google.com/spreadsheets/d/1X4PPWJWRjHKZAigN9RUsZH8fY6lHb3LBM3h-4xr6bb0/edit#gid=1925868699","referral_student_dashbaord",final.astype(int),row = 12,col = 5)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




