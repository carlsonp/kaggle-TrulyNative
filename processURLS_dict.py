from __future__ import print_function
import re, os, sys, multiprocessing, zipfile, pickle
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from urlparse import urlparse
from collections import defaultdict
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar

#Some HTML files are actually NOT in either the training or testing set
process_zips = ["./data/0.zip", "./data/1.zip", "./data/2.zip", "./data/3.zip", "./data/4.zip", "./data/5.zip"]
#process_zips = ["./data/0.zip"]

def parseFile(contents, filename, sponsored):
	urls = []
	#use lxml parser for faster speed
	cleaned = BeautifulSoup(contents, "lxml")
	for anchor in cleaned.findAll('a', href=True):
		if anchor['href'].startswith("http"):
			try:
				parsedurl = urlparse(anchor['href'])
				parsedurl = parsedurl.netloc.replace("www.", "", 1)
				parsedurl = re.sub('[^0-9a-zA-Z\.]+', '', parsedurl) #remove non-alphanumeric and non-period literals
				urls.append(parsedurl)
			except ValueError:
				print("IPv6 URL?")
	#remove duplicate URLS (this will change the order)
	urls = list(set(urls))
	return [sponsored, filename] + urls #combine to single list

def addNodes(nodes):
	for n in nodes[2:]:
		if nodes[0] == 1:
			sponsoredDict[n].append(nodes[1])
		elif nodes[0] == 0:
			notsponsoredDict[n].append(nodes[1])
		elif nodes[0] == 2:
			sampleDict[nodes[1]].append(n)


train = pd.read_csv("./data/train_v2.csv", header=0, delimiter=",", quoting=3)
sample = pd.read_csv("./data/sampleSubmission_v2.csv", header=0, delimiter=",", quoting=3)

print("Starting processing...")

sponsoredDict = defaultdict(list)
notsponsoredDict = defaultdict(list)
sampleDict = defaultdict(list)

for i, zipFile in enumerate(process_zips):			
	archive = zipfile.ZipFile(zipFile, 'r')
	file_paths = zipfile.ZipFile.namelist(archive)
	bar = ProgressBar(len(file_paths), max_width=40)
	pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()-1 or 1)
	for k, file_path in enumerate(file_paths):
		data = archive.read(file_path)
		openfile = file_path[2:] #filename
		res = None
		sponsored = train.loc[train['file'] == openfile]
		if not sponsored.empty:
			res = pool.apply_async(parseFile, args = (data, openfile, int(sponsored['sponsored']), ), callback = addNodes)
		testing = sample.loc[sample['file'] == openfile]
		if not testing.empty:
			res = pool.apply_async(parseFile, args = (data, openfile, 2, ), callback = addNodes)

		if res and len(pool._cache) > 100:
			print("Waiting for cache to clear...")
			res.wait()
		bar.numerator = k
		print("Cache:", len(pool._cache), "Folder:", i, bar, end='\r')
		sys.stdout.flush()
	pool.close()
	pool.join()

print()

print("sponsiredDict size:", sys.getsizeof(sponsoredDict))
print("notsponsiredDict size:", sys.getsizeof(notsponsoredDict))
print("sampleDict size:", sys.getsizeof(sampleDict))

pickle.dump(sponsoredDict, open("sponsoredDict.p", "wb"))
pickle.dump(notsponsoredDict, open("notsponsoredDict.p", "wb"))
pickle.dump(sampleDict, open("sampleDict.p", "wb"))
