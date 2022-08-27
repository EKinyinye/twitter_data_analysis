#!/usr/bin/env python
# coding: utf-8

# # Project: Wrangling and Analyze Data

# ## Data Gathering
# In the cell below, gather **all** three pieces of data for this project and load them in the notebook. **Note:** the methods required to gather each data are different.
# 1. Directly download the WeRateDogs Twitter archive data (twitter_archive_enhanced.csv)

# In[87]:


import pandas as pd
import numpy as np
import requests
import os
import json
import matplotlib as plt
get_ipython().run_line_magic('matplotlib', 'inline')

df_1 = pd.read_csv('twitter-archive-enhanced.csv')


# In[ ]:





# 2. Use the Requests library to download the tweet image prediction (image_predictions.tsv)

# In[3]:


folder_name = 'image_predications'
if not os.path.exists(folder_name):
    os.makedirs(folder_name)
    
url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)
response


# In[4]:


with open(os.path.join(folder_name, url.split('/')[-1]), mode = 'wb') as file:
    file.write(response.content)


# In[5]:


os.listdir(folder_name)


# In[6]:


df_2 = pd.read_csv('image_predications/image-predictions.tsv', sep ='\t')


# In[ ]:





# 3. Use the Tweepy library to query additional data via the Twitter API (tweet_json.txt)

# In[7]:


import tweepy
from tweepy import OAuthHandler
import json
from timeit import default_timer as timer


# In[8]:


# Query Twitter API for each tweet in the Twitter archive and save JSON in a text file
# These are hidden to comply with Twitter's API terms and conditions
consumer_key = 'HIDDEN'
consumer_secret = 'HIDDEN'
access_token = 'HIDDEN'
access_secret = 'HIDDEN'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)


# In[9]:


# NOTE TO STUDENT WITH MOBILE VERIFICATION ISSUES:
# df_1 is a DataFrame with the twitter_archive_enhanced.csv file. You may have to
# change line 17 to match the name of your DataFrame with twitter_archive_enhanced.csv
# NOTE TO REVIEWER: this student had mobile verification issues so the following
# Twitter API code was sent to this student from a Udacity instructor
# Tweet IDs for which to gather additional data via Twitter's API
tweet_ids = df_1.tweet_id.values
len(tweet_ids)


# In[10]:


#Query Twitter's API for JSON data for each tweet ID in the Twitter archive
count = 0 
fails_dict = {} 
start = timer()

#Save each tweet's returned JSON as a new line in a .txt file
with open('tweet_json.txt', 'w') as outfile:

# This loop will likely take 20-30 minutes to run because of Twitter's rate limit
    for tweet_id in tweet_ids:
        count += 1
        print(str(count) + ": " + str(tweet_id))
        try:
                tweet = api.get_status(tweet_id, tweet_mode='extended')
                print("Success")
                json.dump(tweet._json, outfile)
                outfile.write('\n')
        except tweepy.TweepError as e:
                print("Fail")
                fails_dict[tweet_id] = e
                pass

end = timer() 
print(end - start) 
print(fails_dict)


# In[11]:


import json


# In[12]:



df_list =[]
with open('tweet-json.txt','r') as file:
    for line in file:
        data = json.loads(line)
        keys = data.keys()
        id = data['id']
        retweet_count = data['retweet_count']
        favorite_count = data['favorite_count']
        df_list.append({'id':id,
                        'retweet_count':retweet_count,
                        'favorite_count':favorite_count})

df_3 = pd.DataFrame(df_list, columns = ['id', 'retweet_count','favorite_count'])


# In[13]:


df_3.info()


# In[14]:


df_3.head()


# In[ ]:





# In[ ]:





# ## Assessing Data
# In this section, detect and document at least **eight (8) quality issues and two (2) tidiness issue**. You must use **both** visual assessment
# programmatic assessement to assess the data.
# 
# **Note:** pay attention to the following key points when you access the data.
# 
# * You only want original ratings (no retweets) that have images. Though there are 5000+ tweets in the dataset, not all are dog ratings and some are retweets.
# * Assessing and cleaning the entire dataset completely would require a lot of time, and is not necessary to practice and demonstrate your skills in data wrangling. Therefore, the requirements of this project are only to assess and clean at least 8 quality issues and at least 2 tidiness issues in this dataset.
# * The fact that the rating numerators are greater than the denominators does not need to be cleaned. This [unique rating system](http://knowyourmeme.com/memes/theyre-good-dogs-brent) is a big part of the popularity of WeRateDogs.
# * You do not need to gather the tweets beyond August 1st, 2017. You can, but note that you won't be able to gather the image predictions for these tweets since you don't have access to the algorithm used.
# 
# 

# ### Visual assessment
# By running the datasets

# In[15]:


df_1


# In[16]:


df_2


# In[17]:


df_3


# ### Programmatic assessment of the datasets

# #### df_1

# In[18]:


df_1.info()


# In[19]:


df_1.head()


# In[20]:


sum(df_1.duplicated())


# In[21]:


df_1.describe()


# #### df_2

# In[24]:


df_2.info()


# In[25]:


df_2.head()


# In[26]:


sum(df_2.duplicated())


# #### df_3

# In[27]:


df_3.info()


# In[28]:


df_3.head()


# In[29]:


sum(df_3.duplicated())


# ### Quality issues
# df_1
# 1. tweet_id datatype of int iso str 
# 2. Timestamp datatype of object iso of datetime 
# 3. in_reply_to_status_id,in_reply_to_user_id, retweeted_status_id, retweeted_status_user_id,retweeted_status_timestamp have missing values
# 
# Missing values in the doggo, floofer, pupper, puppo labelled as None iso nan 
# 
# Mixed upper and lower values for name, doggo, floofer, pupper and puppo   
# 
# df_2
# 
# 4. tweet_id datatype of int iso str 
# 5. Missing data, 2075 rows iso of 2356 
# 6. Mixed upper and lower names for the p values 
# 
# df_3
# 7. Id column instead of tweet_id 
# 8. id column data type of int iso str 

# ### Tidiness issues
# 1.Four columns of dog stages 
# 
# 2.Three datasets instead of one 

# ## Cleaning Data
# In this section, clean **all** of the issues you documented while assessing. 
# 
# **Note:** Make a copy of the original data before cleaning. Cleaning includes merging individual pieces of data according to the rules of [tidy data](https://cran.r-project.org/web/packages/tidyr/vignettes/tidy-data.html). The result should be a high-quality and tidy master pandas DataFrame (or DataFrames, if appropriate).

# In[30]:


# Make copies of original pieces of data
df_1_clean = df_1.copy()
df_2_clean = df_2.copy()
df_3_clean = df_3.copy()


# ### Issue #1:
# 
# Missing data

# #### Define:
# The original dataset df_1 has 2356 while df_2 has 2075 and df_3 has 2354. This we will not change since we don't have access to the rest of the datasets.
# 
# Dropping the columns with null values
# 
# Rename the None values into nan using np.nan

# #### Code

# In[31]:


df_1_clean.dropna(axis='columns',how='any', inplace=True)


# #### Test

# In[32]:


df_1_clean.info()


# #### Code

# In[33]:


df_1_clean['doggo'].replace('None', np.nan, inplace = True)
df_1_clean['floofer'].replace('None', np.nan, inplace = True)
df_1_clean['pupper'].replace('None', np.nan, inplace = True)
df_1_clean['puppo'].replace('None', np.nan, inplace = True)


# #### Test

# In[34]:


df_1_clean.head()


# ### Issue #2:
# 
# Incorrect Data Types, mispelt 

# #### Define
# 
# Convert ID data types in the there data sets from int to str
# 
# Convert timestamp from obj to datetime
# 
# Also rename id in df_3_clean to tweet_id
# 
# 

# #### Code

# In[35]:


df_3_clean.rename(columns = {'id':'tweet_id'}, inplace = True)


# In[36]:


df_1_clean['tweet_id'] = df_1_clean['tweet_id'].astype(str)
df_2_clean['tweet_id'] = df_2_clean['tweet_id'].astype(str)
df_3_clean['tweet_id'] = df_3_clean['tweet_id'].astype(str)


# In[37]:


df_1_clean['timestamp']= pd.to_datetime(df_1_clean['timestamp'])


# #### Test

# In[38]:


df_1_clean.info()


# In[39]:


df_3_clean.info()


# coverting the strings in the datasets to lower case for consistency in the values

# #### Code

# In[40]:


df_1_clean.name=df_1_clean.name.str.lower()
df_1_clean.doggo=df_1_clean.doggo.str.lower()
df_1_clean.floofer=df_1_clean.floofer.str.lower()
df_1_clean.pupper=df_1_clean.pupper.str.lower()
df_1_clean.puppo=df_1_clean.puppo.str.lower()


# In[41]:


df_2_clean.p1=df_2_clean.p1.str.lower()
df_2_clean.p2=df_2_clean.p2.str.lower()
df_2_clean.p3=df_2_clean.p3.str.lower()


# #### Test

# In[42]:


df_1_clean.head(3)


# In[43]:


df_2_clean.head(3)


# ### Issue #3: Tidiness
# 
# Four columns for the dog stages, that is, doggo, floofer, pupper and puppo

# #### Define
# 

# Combine the the columns into one column

# #### Code

# In[44]:


df_1_clean['dog_stage'] = df_1_clean['text'].str.extract('(doggo|floofer|pupper|puppo)', expand = True)


# In[45]:


df_1_clean.info()


# In[46]:


df_1_clean['dog_stage'].unique()


# In[48]:


df_1_clean.head()


# In[49]:


df_1_clean.drop(columns=['doggo', 'puppo', 'pupper', 'floofer'], axis = 1, inplace = True)


# #### Test

# In[50]:


df_1_clean.info()


# #### Merging the datasets

# In[51]:


df_clean = pd.merge(df_1_clean, df_2_clean,
                            on=['tweet_id'], how='left')


# In[52]:


df_clean = pd.merge(df_clean, df_3_clean,
                            on=['tweet_id'], how='left')


# In[53]:


df_clean.info()


# ## Storing Data
# Save gathered, assessed, and cleaned master dataset to a CSV file named "twitter_archive_master.csv".

# In[60]:


df_clean.to_csv('twitter_archive_master.csv', index = False)


# ## Analyzing and Visualizing Data
# In this section, analyze and visualize your wrangled data. You must produce at least **three (3) insights and one (1) visualization.**

# In[61]:


df = pd.read_csv('twitter_archive_master.csv')


# In[62]:


df.head()


# In[63]:


df.info()


# In[64]:


df['tweet_id'] = df['tweet_id'].astype(str)
df['timestamp']= pd.to_datetime(df['timestamp'])


# In[65]:


df.describe()


# In[68]:


df.dog_stage.value_counts()


# In[81]:


df.groupby('dog_stage')['retweet_count','favorite_count'].mean()


# In[ ]:





# ### Insights:
# 1.  The pupper dog stage had the highest number in terms of occurences.
# 
# 2.  The floofer dog stage had the highest mean for both the retweets and favorites.
# 
# 3. P-vales lie between 0 and 1 because they are confidence interval vales.
# 
# Huge differences between the 75% percentile and the maximum values because few of the retweets and favorites were quite high.

# ### Visualization

# In[101]:


df.groupby('dog_stage').favorite_count.mean().plot(kind='bar', title = 'Favorite_count grouped by dog_stage')


# In[102]:


df.groupby('dog_stage').retweet_count.mean().plot(kind='bar',title = 'Retweet_count grouped by dog_stage')


# In[ ]:





# In[ ]:




