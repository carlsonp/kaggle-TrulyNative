from __future__ import print_function
import re, os, sys, multiprocessing, zipfile, Queue
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from urlparse import urlparse
from py2neo import authenticate, Graph, Node, Relationship
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar

#337304 total HTML files, some are actually NOT in either the training or testing set
#process_zips = ["./data/0.zip", "./data/1.zip", "./data/2.zip", "./data/3.zip", "./data/4.zip"]
process_zips = ["./data/0.zip"]

def parseFile(contents, filename, sponsored):
	nodes = [sponsored, filename]

	#use lxml parser for faster speed
	cleaned = BeautifulSoup(contents, "lxml")
	for anchor in cleaned.findAll('a', href=True):
		if anchor['href'].startswith("http"):
			try:
				parsedurl = urlparse(anchor['href'])
				parsedurl = parsedurl.netloc.replace("www.", "", 1)
				parsedurl = re.sub('[^0-9a-zA-Z\.]+', '', parsedurl) #remove non-alphanumeric and non-period literals
				nodes.append(parsedurl)
			except ValueError:
				print("IPv6 URL?")
	
	return nodes
	
def addNodes(nodes):
	q.put("CREATE(f:files {filename: '"+nodes[1]+"'})")
	if nodes[0] == 1:
		q.put("MATCH (n:roots {type:'Sponsored'}) MATCH (f:files {filename:'"+nodes[1]+"'}) MERGE(n)-[:has]->(f)")
	elif nodes[0] == 0:
		q.put("MATCH (n:roots {type:'NotSponsored'}) MATCH (f:files {filename:'"+nodes[1]+"'}) MERGE(n)-[:has]->(f)")
	else:
		q.put("MATCH (n:roots {type:'Testing'}) MATCH (f:files {filename:'"+nodes[1]+"'}) MERGE(n)-[:has]->(f)")
	for n in nodes[2:]:
		q.put("MERGE (w:website {website:'"+n+"'})") #create website node if it doesn't already exist
		q.put("MATCH (f:files {filename:'"+nodes[1]+"'}) MATCH (w:website {website:'"+n+"'}) MERGE(f)-[:links]->(w)")

train = pd.read_csv("./data/train.csv", header=0, delimiter=",", quoting=3)
sample = pd.read_csv("./data/sampleSubmission.csv", header=0, delimiter=",", quoting=3)

print("Starting processing...")

authenticate("localhost:7474", "neo4j", "neo4j") #username and password
graph = Graph() #by default, py2neo opens localhost
graph.delete_all() #deletes all nodes and edges (clears old data)
tx = graph.cypher.begin()
tx.append("CREATE(n:roots {type: 'Sponsored'})")
tx.append("CREATE(n:roots {type: 'NotSponsored'})")
tx.append("CREATE(n:roots {type: 'Testing'})")
tx.commit()
q = Queue.Queue()

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
		if k > 10:
			tx = graph.cypher.begin()
			while not q.empty():
				tx.append(q.get())
			tx.commit()

		bar.numerator = k
		print("Folder:", i, bar, end='\r')
		sys.stdout.flush()
	pool.close()
	pool.join()
	tx = graph.cypher.begin()
	while not q.empty():
		tx.append(q.get())
	tx.commit()

print()



#print("Sponsored pages: ", G.out_degree("SPONSORED"))
#print("Normal pages: ", G.out_degree("NOTSPONSORED"))

#if G.out_degree("TESTING") != 235917:
	#print("Error, invalid number of testing nodes.")
	
#if G.out_degree("SPONSORED") + G.out_degree("NOTSPONSORED") != 101107:
	#print("Error, invalid number of training nodes.")

