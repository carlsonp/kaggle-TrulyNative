from __future__ import print_function
import re, os, sys
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from urlparse import urlparse
import networkx as nx
import matplotlib.pyplot as plt
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar

#337304 total HTML files
process_folders = ["./data/0/", "./data/1/", "./data/2/", "./data/3/", "./data/4/"]

def saveURLS(filename, text):
	#print "Creating node: ", filename
	G.add_node(filename)
	
	#use lxml parser for faster speed
	cleaned = BeautifulSoup(text, "lxml")
	for anchor in cleaned.findAll('a', href=True):
		if anchor['href'].startswith("http"):
			try:
				parsedurl = urlparse(anchor['href'])
				parsedurl = parsedurl.netloc.replace("www.", "", 1)
				if parsedurl not in G:
					#print "Adding: ", parsedurl
					G.add_node(parsedurl)
					G.add_edge(filename, parsedurl)
			except ValueError:
				print("IPv6 URL?")

#use Pandas for reading file
train = pd.read_csv("./data/train.csv", header=0, delimiter=",", quoting=3)


print("Starting processing...")

G = nx.Graph()

for i, folder in enumerate(process_folders):
	onlyfiles = []
	for root, dirs, files in os.walk(os.path.abspath(folder)):
		for file in files:
			onlyfiles.append(os.path.join(root, file))
	bar = ProgressBar(len(onlyfiles), max_width=40)
	for k, openfile in enumerate(onlyfiles):
		sponsored = train.loc[train['file'] == os.path.basename(openfile)]
		if not sponsored.empty:
			f = open(openfile, 'r')
			contents = f.read()
			saveURLS(os.path.basename(openfile), contents)
			f.close()
			bar.numerator = k
			print("Folder:", i, bar, end='\r')
			sys.stdout.flush()
print()

print(nx.info(G))
