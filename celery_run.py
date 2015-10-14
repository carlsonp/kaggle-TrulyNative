"""

Beating the Benchmark
Truly Native?
__author__ : David Shinn, modified by firefly2442

"""
from __future__ import print_function

from collections import Counter, defaultdict
import glob, os, re, sys, time
from celery_tasks import processFile
from celery.result import ResultSet

from sklearn.ensemble import RandomForestClassifier
import pandas as pd
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar


print('--- Read training labels')
train = pd.read_csv('./data/train_v2.csv')
train_keys = dict([a[1] for a in train.iterrows()])
test_files = set(pd.read_csv('./data/sampleSubmission_v2.csv').file.values)

print("--- Started processing")
result = ResultSet([])
bar = ProgressBar(len(train)+len(test_files), max_width=40)
#https://celery.readthedocs.org/en/latest/reference/celery.result.html#celery.result.ResultSet
for k, filename in enumerate(list(train['file'])+list(test_files)):
	if filename in train_keys:
		result.add(processFile.delay(filename, train_keys[filename]))
	elif filename != "":
		result.add(processFile.delay(filename, 2))
	
	#sponsored = train.loc[train['file'] == openfile]
	#if not sponsored.empty:
		#result.add(processFile.delay(openfile, data, int(sponsored['sponsored'])))
	#testing = sample.loc[sample['file'] == openfile]
	#if not testing.empty:
		#result.add(processFile.delay(openfile, data, int(sponsored['sponsored'])))


	bar.numerator = k
	print("Sending out processes ", bar, end='\r')
	sys.stdout.flush()

bar = ProgressBar(len(train)+len(test_files), max_width=40)
while not result.ready():
	time.sleep(5)
	bar.numerator = result.completed_count()
	print("Waiting for return results ", bar, end='\r')
	sys.stdout.flush()

results = result.join() #wait for jobs to finish

df_full = pd.DataFrame(list(results))

print('--- Training random forest')
clf = RandomForestClassifier(n_estimators=150, n_jobs=-1, random_state=0)
train_data = df_full[df_full.sponsored.notnull()].fillna(0)
test = df_full[df_full.sponsored.isnull() & df_full.file.isin(test_files)].fillna(0)
clf.fit(train_data.drop(['file', 'sponsored'], 1), train_data.sponsored)

print('--- Create predictions and submission')
submission = test[['file']].reset_index(drop=True)
submission['sponsored'] = clf.predict_proba(test.drop(['file', 'sponsored'], 1))[:, 1]
submission.to_csv('native_btb_basic_submission.csv', index=False)
