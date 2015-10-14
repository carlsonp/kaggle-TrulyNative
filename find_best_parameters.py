from __future__ import print_function
import pickle, os, sys, glob, hashlib

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_curve, auc
from sklearn.cross_validation import train_test_split
import pandas as pd
import numpy as np

test_files = set(pd.read_csv('./data/sampleSubmission_v2.csv').file.values)
train = pd.read_csv('./data/train_v2.csv')


df_full = pickle.load(open( "df_full.p", "rb"))


#no point using empty files in our training set so we remove them
print('--- Removing empty files')
filepaths = glob.glob('data/*/*.txt')
for filepath in filepaths:
	if os.path.getsize(filepath) == 0:
		filename = os.path.basename(filepath)
		df_full = df_full[df_full.file != filename]
		if filename in test_files:
			print("Found empty file in submission: ", filename)


train_data = df_full[df_full.sponsored.notnull()].fillna(0)
test = df_full[df_full.sponsored.isnull() & df_full.file.isin(test_files)].fillna(0)
# shuffle and split training and test sets
X_train, X_test, y_train, y_test = train_test_split(train_data.drop(['file', 'sponsored'], 1), train_data.sponsored, random_state=0, test_size=.25)


#https://www.youtube.com/watch?v=0GrciaGYzV0

#calculate AUC score:
#http://www.ultravioletanalytics.com/2014/12/16/kaggle-titanic-competition-part-x-roc-curves-and-auc/
#http://scikit-learn.org/stable/auto_examples/model_selection/plot_roc.html

#Go through many possible parameter configurations for random forest to see
#which ones have the best AUC score.

auc_results = pd.DataFrame()
for c in ["gini", "entropy"]:
	for max_f in np.arange(0.0, 1, 0.05):
		if max_f == 0.0:
			max_f = None
		#for min_s_split in range(1, 10, 2):
			#for min_s_leaf in range(1, 10, 2):
				#for min_w_f_leaf in np.arange(0, 0.5, 0.1):
		for bs in [True, False]:
			clf = RandomForestClassifier(n_estimators=10, n_jobs=-1, random_state=0,
			criterion=c,
			max_features=max_f,
			#min_samples_split=min_s_split,
			#min_samples_leaf=min_s_leaf,
			#min_weight_fraction_leaf=min_w_f_leaf,
			bootstrap=bs)
			clf.fit(X_train, y_train)
			# Determine the false positive and true positive rates
			fpr, tpr, _ = roc_curve(y_test, clf.predict_proba(X_test)[:,1])
			# Calculate the AUC
			roc_auc = auc(fpr, tpr)
			series = pd.Series({'criterion':c,
								'max_features':max_f,
								#'min_samples_split':min_s_split,
								#'min_samples_leaf':min_s_leaf,
								#'min_weight_fraction_leaf':min_w_f_leaf,
								'bootstrap':bs,
								'AUC':roc_auc
								})
			auc_results = auc_results.append(series, ignore_index=True)
			print(roc_auc)


print(auc_results.shape)
auc_results.to_csv('auc_results.csv', index=False)
