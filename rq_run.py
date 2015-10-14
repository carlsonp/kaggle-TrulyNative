"""

Beating the Benchmark
Truly Native?
__author__ : David Shinn, modified by firefly2442

"""
from __future__ import print_function

from collections import Counter, defaultdict
import glob, os, re, sys, time, pickle, random
from bs4 import BeautifulSoup
from urlparse import urlparse

import pandas as pd
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar

from rq import Queue
from redis import Redis
from rq_task import processFile


print('--- Read training labels')
train = pd.read_csv('./data/train_v2.csv')
train_keys = dict([a[1] for a in train.iterrows()])
test_files = set(pd.read_csv('./data/sampleSubmission_v2.csv').file.values)


filepaths = glob.glob('data/*/*.txt')

#random.shuffle(filepaths)
#filepaths = filepaths[0:1000]


bar = ProgressBar(len(filepaths), max_width=40)
print("--- Started processing")
redis_conn = Redis('192.168.1.140', 6379)
q = Queue("low", connection=redis_conn)
jobs = []
for i, filepath in enumerate(filepaths):
	bar.numerator = i
	filename = os.path.basename(filepath)
	if filename in train_keys:
		jobs.append(q.enqueue_call(func=processFile, args=(filepath, train_keys[filename],), timeout=300))
	else:
		jobs.append(q.enqueue_call(func=processFile, args=(filepath, 2,), timeout=300))
	print("Adding jobs to queue", bar, end='\r')
	sys.stdout.flush()
print()

print("--- Waiting for return results")
bar = ProgressBar(len(filepaths), max_width=40)
results = []
bar.numerator = 1
while len(jobs) != 0:
	for j in jobs:
		if j.is_finished:
			#TODO: this seems to freeze at the end, is there a better way to do this?
			results.append(j.result)
			jobs.remove(j)
			bar.numerator += 1
			print("Pulling results from queue", bar, end='\r')
			sys.stdout.flush()
	time.sleep(5)
print()


df_full = pd.DataFrame(results)

print("--- Calculating link ratios")
#calculate counts for training data
sponsored_counts = {}
nonsponsored_counts = {}
for index, row in df_full.iterrows():
	for url in row['sponsored_links']:
		if url in sponsored_counts:
			sponsored_counts[url] += 1
		else:
			sponsored_counts[url] = 1
	for url in row['nonsponsored_links']:
		if url in nonsponsored_counts:
			nonsponsored_counts[url] += 1
		else:
			nonsponsored_counts[url] = 1

df_full['sponsored_ratio'] = None #create empty column in dataframe
#calculate ratio of sponsored to nonsponsored links
for index, row in df_full.iterrows():
	websites = row['sponsored_links'] + row['nonsponsored_links'] + row['unknown_links']
	l = []
	for website in websites:
		sponsoredCount = float(0)
		notsponsoredCount = float(0)
		if website in sponsored_counts:
			sponsoredCount = float(sponsored_counts.get(website))
		if website in nonsponsored_counts:
			notsponsoredCount = float(nonsponsored_counts.get(website))
		ratio = float(0)
		if sponsoredCount != 0:
			ratio = sponsoredCount / (sponsoredCount + notsponsoredCount)
		l.append(ratio)
	if len(l) != 0:
		df_full.set_value(index, 'sponsored_ratio', sum(l)/len(l))
	else:
		df_full.set_value(index, 'sponsored_ratio', 0)
		
		
#remove old data that we don't need for the prediction
df_full = df_full.drop(['sponsored_links', 'nonsponsored_links', 'unknown_links'], 1)

pickle.dump(df_full, open("df_full.p", "wb"))

