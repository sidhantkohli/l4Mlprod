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
upstream = ['data_clean']

# This is a placeholder, leave it as None
product = None


# %%
# your code here...import numpy as np
import pandas as pd
import numpy as np
from surprise import Reader
from surprise import KNNWithMeans
from surprise import Dataset
import pickle
from surprise.model_selection import cross_validate
from surprise import accuracy, Dataset, SVD
from sklearn.model_selection import train_test_split
from surprise.model_selection import GridSearchCV
from surprise import SVD
import pickle
import os
import glob


# %%
def process_data(dirname):
    # current_path = os.getcwd()
    # os.chdir(dirname)
    # extension = 'csv'
    # all_filenames = [i for i in glob.glob('watched_rated*.{}'.format(extension))]
    # movie_df_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    movie_df_csv = pd.read_csv (dirname)
    # movie_df_csv = dirname
    # os.chdir(current_path)
    movie_df_csv.to_csv(product['train'], index=False, encoding='utf-8-sig')
    df_sub = movie_df_csv[['userid','movieid','rating']]
    train, test = train_test_split(df_sub, test_size=0.2)
#    os.path.join( os.path.dirname(os.getcwd()), '..' )
    print("Done with train test split")
    return train,test

def load(train,test):
    reader = Reader(rating_scale=(1, 5))    
    data_train = Dataset.load_from_df(train[['userid','movieid','rating']], reader)
    trainingSet = data_train.build_full_trainset()
    print("Done Processing")
    return trainingSet, data_train, test


# %%
def modelling(trainingSet, data_train, test):
   param_grid = {
    "n_epochs": [5, 10],
    "lr_all": [0.002, 0.005],
    "reg_all": [0.4, 0.6]
    }

    # Get the best params using GridSearchCV
   gs = GridSearchCV(SVD, param_grid, measures=["rmse"], cv=4)
   gs.fit(data_train)
   best_params = gs.best_params["rmse"]
   print(gs.best_score["rmse"])
   print(gs.best_params["rmse"])
    
   # Extract and train model with best params
   svd_algo = SVD(n_epochs=best_params['n_epochs'],
                   lr_all=best_params['lr_all'],
                   reg_all=best_params['reg_all'])
   svd_algo.fit(trainingSet)

   predictions = []
   actuals = []
   rmse_val = []
   for col, row in test.iterrows():
       predictions.append(svd_algo.predict(row.userid, row.movieid).est)
       actuals.append(row.rating)
   rmse_val = rmse(np.array(predictions), np.array(actuals))
   print("Test RMSE for SVD : " + str(rmse_val))
   print("Done Modelling")
   return svd_algo


# %%
def model_to_binary(svd_model):
    filename = product['data']
    outfile = open(filename,'wb')
    pickle.dump(svd_model,outfile)
    outfile.close()      
    print("Done Converting to Binary")

    #this function returns the root mean squared error between two arrays
def rmse(predictions, targets):
    return np.sqrt(((predictions - targets) ** 2).mean())

def main():
    data_dir = "/home/sidhantk/pl/movie/watched_rated_df.csv"
    # data_dir = upstream['data_clean']['data']
    train,test=process_data(data_dir)
    trainingSet, data_train, test = load(train,test)
    svd_model = modelling(trainingSet, data_train, test)
    model_to_binary(svd_model)


# %%
main()

# %%

# %%
