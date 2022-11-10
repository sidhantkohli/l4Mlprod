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
upstream = None

# This is a placeholder, leave it as None
product = None


# %%
# your code here...
from os import path
import sys, os
from datetime import datetime
from json import dumps, loads
from time import sleep
from random import randint
import numpy as np
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer


# %%
topic = 'movielog2'
def createConsumer(offset_reset):
    # Create a consumer to read data from kafka
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        # Read from the start of the topic; Default is latest
        auto_offset_reset=offset_reset,
        # auto_offset_reset='latest',
        group_id='team2',
        # Commit that an offset has been read
        enable_auto_commit=True,
        # How often to tell Kafka, an offset has been read
        # auto_commit_interval_ms=1000
    )
    return consumer


# %%
def fetchDataKafka(log_file, offset_reset, consume_time):
    try:
        kafka_consumer = createConsumer(offset_reset=offset_reset)
    except Exception as e:
        print(e)
        return False
    print('Reading Kafka Broker')
    end_time = datetime.now() + timedelta(seconds=consume_time)
    for message in kafka_consumer:
        message = message.value.decode('utf-8')
        # Default message.value type is bytes!
        os.system(f"echo {message} >> {log_file}")
        if datetime.now() >= end_time:
            break
    return True


# %%
fetchDataKafka(product['data'], "earliest", 60)

# %%
