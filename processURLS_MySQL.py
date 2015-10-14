from __future__ import print_function
import re, os, sys, multiprocessing, zipfile
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from urlparse import urlparse
import MySQLdb
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
	if nodes[0] == 1:
		cur.execute("INSERT into files(filename, type) VALUES('"+nodes[1]+"', '1');")
	elif nodes[0] == 0:
		cur.execute("INSERT into files(filename, type) VALUES('"+nodes[1]+"', '0');")
	else:
		cur.execute("INSERT into files(filename, type) VALUES('"+nodes[1]+"', '2');")
	fileID = str(cur.lastrowid)
	for n in nodes[2:]:
		cur.execute("INSERT IGNORE into websites(website) VALUES('"+n+"');")
		websiteID = str(cur.lastrowid)
		cur.execute("INSERT into links(fileID, websiteID) VALUES('"+fileID+"', '"+websiteID+"');")
	db.commit()

train = pd.read_csv("./data/train.csv", header=0, delimiter=",", quoting=3)
sample = pd.read_csv("./data/sampleSubmission.csv", header=0, delimiter=",", quoting=3)

print("Starting processing...")

db = MySQLdb.connect(host="localhost",
                     user="kaggle",
                      passwd="kaggle",
                      db="kaggle")
cur = db.cursor()
cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
cur.execute("DELETE FROM files;")
cur.execute("DELETE FROM websites;")
cur.execute("DELETE FROM links;")
db.commit()

for i, zipFile in enumerate(process_zips):			
	archive = zipfile.ZipFile(zipFile, 'r')
	file_paths = zipfile.ZipFile.namelist(archive)
	bar = ProgressBar(len(file_paths), max_width=40)
	pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()-1 or 1)
	for k, file_path in enumerate(file_paths):
		data = archive.read(file_path)
		openfile = file_path[2:] #filename
		sponsored = train.loc[train['file'] == openfile]
		if not sponsored.empty:
			pool.apply_async(parseFile, args = (data, openfile, int(sponsored['sponsored']), ), callback = addNodes)
		testing = sample.loc[sample['file'] == openfile]
		if not testing.empty:
			pool.apply_async(parseFile, args = (data, openfile, 2, ), callback = addNodes)

		bar.numerator = k
		print("Folder:", i, bar, end='\r')
		sys.stdout.flush()
	pool.close()
	pool.join()

print()

cur.close()
db.close()

#print("Sponsored pages: ", G.out_degree("SPONSORED"))
#print("Normal pages: ", G.out_degree("NOTSPONSORED"))

#if G.out_degree("TESTING") != 235917:
	#print("Error, invalid number of testing nodes.")
	
#if G.out_degree("SPONSORED") + G.out_degree("NOTSPONSORED") != 101107:
	#print("Error, invalid number of training nodes.")

