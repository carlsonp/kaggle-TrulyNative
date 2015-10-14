from __future__ import print_function
import re, os, sys, multiprocessing, zipfile
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from urlparse import urlparse
import networkx as nx
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar

#337304 total HTML files, some are actually NOT in either the training or testing set
#process_zips = ["./data/0.zip", "./data/1.zip", "./data/2.zip", "./data/3.zip", "./data/4.zip"]
process_zips = ["./data/0.zip"]

def parseFile(contents, filename, sponsored):
	header = [sponsored, filename]
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
	return header + urls #combine to single list
	
def addNodes(nodes):
	G.add_node(nodes[1]) #filename
	if nodes[0] == 1:
		G.add_edge("Sponsored", nodes[1])
	elif nodes[0] == 0:
		G.add_edge("NotSponsored", nodes[1])
	else:
		G.add_edge("Testing", nodes[1])
	for n in nodes[2:]:
		if n not in G:
			G.add_node(n)
		G.add_edge(nodes[1], n)


train = pd.read_csv("./data/train.csv", header=0, delimiter=",", quoting=3)
sample = pd.read_csv("./data/sampleSubmission.csv", header=0, delimiter=",", quoting=3)

print("Starting processing...")

G = nx.DiGraph()
G.add_node("Sponsored")
G.add_node("NotSponsored")
G.add_node("Testing")

for i, zipFile in enumerate(process_zips):			
	archive = zipfile.ZipFile(zipFile, 'r')
	file_paths = zipfile.ZipFile.namelist(archive)
	bar = ProgressBar(len(file_paths), max_width=40)
	pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()-1 or 1)
	for k, file_path in enumerate(file_paths):
		data = archive.read(file_path)
		openfile = file_path[2:] #filename
		sponsored = train.loc[train['file'] == openfile]
		res = None
		if not sponsored.empty:
			res = pool.apply_async(parseFile, args = (data, openfile, int(sponsored['sponsored']), ), callback = addNodes)
		testing = sample.loc[sample['file'] == openfile]
		if not testing.empty:
			res = pool.apply_async(parseFile, args = (data, openfile, 2, ), callback = addNodes)

		if res is not None and len(pool._cache) > 1000:
			print("Waiting for cache to clear...")
			res.wait()
		bar.numerator = k
		print("Folder:", i, bar, end='\r')
		sys.stdout.flush()
	pool.close()
	pool.join()

print()


print(nx.info(G))
