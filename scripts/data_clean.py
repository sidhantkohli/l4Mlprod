# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# Add description here
#
# *Note:* You can open this file as a notebook (JupyterLab: right-click on it in the side bar -> Open With -> Notebook)


# %%
# Uncomment the next two lines to enable auto reloading for imported modules
# # %load_ext autoreload
# # %autoreload 2
# For more info, see:
# https://docs.ploomber.io/en/latest/user-guide/faq_index.html#auto-reloading-code-in-jupyter

# %% tags=["parameters"]
# If this task has dependencies, list them them here
# (e.g. upstream = ['some_task']), otherwise leave as None.
upstream = ['data_fetch']

# This is a placeholder, leave it as None
product = None


# %%
import re
import pandas as pd
from collections import defaultdict
import timeit
import datetime
# your code here...

# %%
def validate_datetime(date_text):
    """ Returns True is date is a valid date """
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%dT%H:%M:%S')
        return True
    except ValueError:
        return False



# %%
def validate_integer(user_id_text):
    """ Returns True is string is a number > 0 """
    return user_id_text.isdigit()


# %%
def validate_request(request_text):
    if 'GET /data/m/' in request_text:

        try :
            parsed = re.split("\/(.*)\/(.*)\/(.*)\.",request_text)
            
            # Check if movie watched minute is an integer > 0
            if validate_integer(parsed[3]):
                return True
            else: 
                return False  

        except:
            return False


    elif 'GET /rate/' in request_text:

        try :
            parsed = re.split("=",re.split("\/",request_text)[2])
            
            # Check if rating is an integer
            if validate_integer(parsed[1]) and 0<int(parsed[1])<=5:
                return True
            else: 
                return False 

        except:
            return False      

    else:
        return False 


# %%
def data_quality_check(line):
    # Number of data cols in the line
    line_data = line.strip().split(",")
    
    if len(line_data) == 3:
        
        if validate_datetime(line_data[0]) and validate_integer(line_data[1]) \
        and validate_request(line_data[2]):
            return True
        else: 
            return False

    elif len(line_data) == 25:

        # Check for Error Status messages
        if '200' not in line_data[3]:
            return False
        
        if validate_datetime(line_data[0].split('.')[0]) and validate_integer(line_data[1]):
            return True
        else: 
            return False                 

    else:
        return False   


# %%
def data_clean(file_name):

    start = timeit.default_timer()
    curTimeStamp = datetime.datetime.now()
    str_curTimeStamp = str(curTimeStamp.date())
    dct1,dct2,dct3,idx1,idx2 = defaultdict(),{},{},0,0
    # recs_path = f"files/recs_{str_curTimeStamp}.csv"
    # watched_path = f"files/watched_{str_curTimeStamp}.csv"
    # watched_rated_path = f"files/watched_rated_{str_curTimeStamp}.csv"
    watched_path = product['data']
    
    
    success_count, error_count = 0, 0

    watch_cols = ["userid","movieid","date","time","minutes"]
    rate_cols = ["userid","movieid","date","time","rating"]
    recs_cols = ["userid","movielist","date","time","latency"]
    
    # Open kafka consumer file in read mode
    with open(file_name, 'r') as fp:
        for line in fp:
            
            # Quality Check
            if data_quality_check(line) == False:
                error_count+=1
                continue
            else: 
                success_count+=1

            # watching
            if 'GET /data/m/' in line:
                # Simple parsing: date, time, userid
                ts,user,log = line.strip().split(",")
                day,time = ts.split("T")

                # parse system log to get <movieid> & <watching_minute>
                parsed = re.split("\/(.*)\/(.*)\/(.*)\.",log)
                # print(parsed)
                movie,minutes = parsed[2],parsed[3]

                # (1) if we want to get all movie logs
                # lst = [d,t,user,movieid,minutes]
                # row_dict = dict(zip(watch_cols, lst))

                # (2) if we want to keep only one record for each watching
                ks = ','.join([user,movie,day])
                vs = ','.join([time,minutes])

                if ks not in dct1:
                    dct1[ks] = vs
                else:
                    # get the duration of watching: keep the maximum number of minutes here
                    if minutes > dct1.get(ks):
                        dct1[ks] = vs
            
            # rating
            if 'GET /rate/' in line:
                # Simple parsing: date, time, userid
                ts,user,log = line.strip().split(",")
                day,time = ts.split("T")

                # parse system log to get <movieid> & <rating>
                parsed = re.split("=",re.split("\/",log)[2])
                movieid,rating = parsed[0],parsed[1]

                # append result to the dictionary
                lst = [user,movieid,day,time,rating]
                row_dict = dict(zip(rate_cols, lst))
                dct2[idx1] = row_dict
                idx1 += 1
            
            # recommendation
            if 'recommendation request' in line:
                # Simple parsing: date, time, userid, movielist, latency
                data, recs_latency= line.strip().split(", result: ")
                ts,user,log,_ = data.strip().split(",")
                lastCommaIndex = recs_latency.rfind(",")
                recs = recs_latency[0: lastCommaIndex] 
                latency = recs_latency[lastCommaIndex + 1:]
                day,time = ts.split("T")
                time_without_ms = time.split('.')[0]

                # append result to the dictionary
                lst = [user,recs,day,time_without_ms,latency]
                row_dict = dict(zip(recs_cols, lst))
                dct3[idx2] = row_dict
                idx2 += 1
                                    

    values = [('{},{}'.format(key, value)).split(",") for key, value in dct1.items()]
    watch_df = pd.DataFrame(values, columns = watch_cols)
    rate_df = pd.DataFrame.from_dict(dct2, orient='index')
    recs_df = pd.DataFrame.from_dict(dct3, orient='index')

    # inner join two tables for user BOTH watched AND rated the movie
    both_df = pd.merge(
                    watch_df, 
                    rate_df, 
                    how="inner", 
                    on=["userid", "movieid"],
                    suffixes=('_watch','_rate'))

    # output to file if not test
    if file_name != "sample_test.log":
        # watch_df.to_csv(watched_path,sep=",",index=False)
        # rate_df.to_csv(f"files/rated_{str_curTimeStamp}.csv",sep=",",index=False)
        both_df.to_csv(watched_path, sep=",",index=False)
        # recs_df.to_csv(recs_path, sep=",",index=False)

    stop = timeit.default_timer()
    print('Time: ', stop - start)

    return success_count, error_count, watched_path

# %%
data_clean(upstream['data_fetch']['data'])

# %%
