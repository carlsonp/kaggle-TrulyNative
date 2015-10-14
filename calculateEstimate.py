from __future__ import print_function
import re, os, sys, multiprocessing, zipfile, pickle
import pandas as pd
import numpy as np
from collections import defaultdict
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar

#Some HTML files are actually NOT in either the training or testing set
#5.zip is the testing data
process_zips = ["./data/5.zip"]

def calcEstimate(filename, websites):
	l = []
	for website in websites:
		sponsoredCount = float(0)
		notsponsoredCount = float(0)
		if sponsoredDict.get(website):
			sponsoredCount = float(len(sponsoredDict.get(website)))
		if notsponsoredDict.get(website):
			notsponsoredCount = float(len(notsponsoredDict.get(website)))
		ratio = float(0)
		if sponsoredCount != 0:
			ratio = sponsoredCount / (sponsoredCount + notsponsoredCount)
		l.append(ratio)
	if len(l) != 0:
		average = sum(l)/len(l)
		return [filename, average]
	return []
		
def saveAverage(d):
	if d:
		index = sample.loc[sample['file'] == d[0]].index
		sample.set_value(index, "sponsored", d[1])


print("Starting processing test data...")

sample = pd.read_csv("./data/sampleSubmission_v2.csv", header=0, delimiter=",", quoting=3)

sponsoredDict = pickle.load(open( "sponsoredDict.p", "rb")) #key: website, value: filenames
notsponsoredDict = pickle.load(open( "notsponsoredDict.p", "rb")) #key: website, value: filenames
sampleDict = pickle.load(open( "sampleDict.p", "rb")) #key: filename, value: websites


for i, zipFile in enumerate(process_zips):
	archive = zipfile.ZipFile(zipFile, 'r')
	file_paths = zipfile.ZipFile.namelist(archive)
	bar = ProgressBar(len(file_paths), max_width=40)
	pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()-1 or 1)
	for k, file_path in enumerate(file_paths):
		data = archive.read(file_path)
		openfile = file_path[2:] #filename
		testing = sample.loc[sample['file'] == openfile]
		res = None
		if not testing.empty and sampleDict.get(openfile):
			res = pool.apply_async(calcEstimate, args = (openfile, sampleDict.get(openfile), ), callback = saveAverage)

		if res and len(pool._cache) > 100:
			print("Waiting for cache to clear...")
			res.wait()
		bar.numerator = k
		print("Cache:", len(pool._cache), "Folder:", i, bar, end='\r')
		sys.stdout.flush()
	pool.close()
	pool.join()


print()

sample.to_csv("mySubmission_v2.csv", header=True, index=False, sep=',')
