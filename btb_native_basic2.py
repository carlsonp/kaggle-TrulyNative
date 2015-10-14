"""

Beating the Benchmark
Truly Native?
__author__ : David Shinn, modified by firefly2442

"""
from __future__ import print_function

import glob, multiprocessing, os, re, sys, time, pickle, random
from bs4 import BeautifulSoup
from urlparse import urlparse

import pandas as pd
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar

print('--- Read training labels')
train = pd.read_csv('./data/train_v2.csv')
train_keys = dict([a[1] for a in train.iterrows()])


def create_data(filepath):
	values = {}
	urls = set() #this way we don't have duplicate URLs
	filename = os.path.basename(filepath)
	with open(filepath, 'rb') as infile:
		text = infile.read()

	values['file'] = filename
	if filename in train_keys:
		values['sponsored'] = train_keys[filename]
	values['lines'] = text.count('\n')
	values['spaces'] = text.count(' ')
	values['tabs'] = text.count('\t')
	values['braces'] = text.count('{')
	values['brackets'] = text.count('[')
	values['words'] = len(re.split('\s+', text))
	values['length'] = len(text)

	#use lxml parser for faster speed
	parsed = BeautifulSoup(text, "lxml")
	cleaned = parsed.getText()
	values['cleaned_lines'] = cleaned.count('\n')
	values['cleaned_spaces'] = cleaned.count(' ')
	values['cleaned_tabs'] = cleaned.count('\t')
	values['cleaned_words'] = len(re.split('\s+', cleaned))
	values['cleaned_length'] = len(cleaned)

	values['http_total_links'] = 0
	values['other_total_links'] = 0
	for anchor in parsed.findAll('a', href=True):
		if anchor['href'].startswith("http"):
			values['http_total_links'] += 1
			try:
				parsedurl = urlparse(anchor['href'])
				parsedurl = parsedurl.netloc.replace("www.", "", 1)
				parsedurl = re.sub('[^0-9a-zA-Z\.]+', '', parsedurl) #remove non-alphanumeric and non-period literals
				urls.add(parsedurl)
			except ValueError:
				print("IPv6 URL?")
		else:
			values['other_total_links'] += 1

	values['http_pruned_links'] = len(urls)

	values['sponsored_links'] = []
	values['nonsponsored_links'] = []
	values['unknown_links'] = []
	if filename in train_keys:
		if train_keys[filename] == 1:
			values['sponsored_links'] = list(urls)
		elif train_keys[filename] == 0:
			values['nonsponsored_links'] = list(urls)
	else:
		values['unknown_links'] = list(urls)

	if parsed.title and parsed.title.string:
		values['title_length'] = len(parsed.title.string)
		
	javascript = parsed.findAll('script')
	values['javascript_scripts_count'] = len(javascript)
	values['javascript_length'] = 0
	for j in javascript:
		values['javascript_length'] += len(j)
		
	values['img_count'] = len(parsed.findAll('img'))
	
	return values	


filepaths = glob.glob('data/*/*.txt')

#random.shuffle(filepaths)
#filepaths = filepaths[0:1000]

num_tasks = len(filepaths)


bar = ProgressBar(num_tasks, max_width=40)
p = multiprocessing.Pool()
results = p.imap(create_data, filepaths)
print("--- Started processing")
while (True):
	bar.numerator = results._index
	print(bar, end='\r')
	sys.stdout.flush()
	time.sleep(1)
	if (results._index == num_tasks): break
p.close()
p.join()
print()

df_full = pd.DataFrame(list(results))

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

