"""

Beating the Benchmark
Truly Native?
__author__ : David Shinn, modified by firefly2442

"""
from __future__ import print_function

import glob, multiprocessing, os, re, sys, time, pickle, random
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar
#https://pypi.python.org/pypi/tld/
from tld import get_tld



class ResultFileEntry(object):
	#https://utcc.utoronto.ca/~cks/space/blog/python/WhatSlotsAreGoodFor
	#http://tech.oyster.com/save-ram-with-python-slots/
	#https://stackoverflow.com/questions/1336791/dictionary-vs-object-which-is-more-efficient-and-why
	#https://stackoverflow.com/questions/472000/python-slots
	# We use slots here to save memory, a dynamic dictionary is not needed
	__slots__ = ["file", "sponsored", "lines", "spaces", "tabs", "braces", "brackets", "parentheses", "words", "length", "cleaned_lines",
				"cleaned_spaces", "cleaned_tabs", "cleaned_words", "cleaned_length", "http_total_links", "other_total_links",
				"domain_com", "domain_edu", "domain_gov", "domain_org", "domain_net", "http_pruned_links", "sponsored_links",
				"nonsponsored_links", "unknown_links", "title_length", "javascript_scripts_count", "javascript_length",
				"img_count", "meta_length", "meta_keywords", "meta_description_length", "meta_redirect", "iframe_count", "word_ad", "word_sponsored",
				"word_advertisement", "word_money", "word_cash", "word_prize", "word_buy", "word_sell", "word_cookie", "word_redirect",
				"word_free", "word_affiliate", "word_banner", "word_scentsy", "word_adword", "word_marketing", "word_track", "robot",
				"XMLHttpRequest_count", "js_redirect", "ajax", "document_write", "url_behance", "url_incapsula", "url_godaddy_park",
				"url_amazon"]
	def __init__(self, filename):
		self.file = filename
		self.sponsored = None #make sure this is none because there is a check for this later during training
		self.lines = 0
		self.spaces = 0
		self.tabs = 0
		self.braces = 0
		self.brackets = 0
		self.parentheses = 0
		self.words = 0
		self.length = 0
		self.cleaned_lines = 0
		self.cleaned_spaces = 0
		self.cleaned_tabs = 0
		self.cleaned_words = 0
		self.cleaned_length = 0
		self.http_total_links = 0
		self.other_total_links = 0
		self.domain_com = 0
		self.domain_edu = 0
		self.domain_gov = 0
		self.domain_org = 0
		self.domain_net = 0
		self.http_pruned_links = 0
		self.sponsored_links = []
		self.nonsponsored_links = []
		self.unknown_links = []
		self.title_length = 0
		self.javascript_scripts_count = 0
		self.javascript_length = 0
		self.img_count = 0
		self.meta_length = 0
		self.meta_keywords = 0
		self.meta_description_length = 0
		self.meta_redirect = 0
		self.iframe_count = 0
		self.word_ad = 0
		self.word_sponsored = 0
		self.word_advertisement = 0
		self.word_money = 0
		self.word_cash = 0
		self.word_prize = 0
		self.word_buy = 0
		self.word_sell = 0
		self.word_cookie = 0
		self.word_redirect = 0
		self.word_free = 0
		self.word_affiliate = 0
		self.word_banner = 0
		self.word_scentsy = 0
		self.word_adword = 0
		self.word_marketing = 0
		self.word_track = 0
		self.robot = 0 #robots.txt information
		self.XMLHttpRequest_count = 0
		self.js_redirect = 0
		self.ajax = 0
		self.document_write = 0
		self.url_behance = 0
		self.url_incapsula = 0
		self.url_godaddy_park = 0
		self.url_amazon = 0

	#https://docs.python.org/2.7/reference/datamodel.html#special-method-names
	def __iter__(self):
		yield self.file;
		yield self.sponsored;
		yield self.lines;
		yield self.spaces;
		yield self.tabs;
		yield self.braces;
		yield self.brackets;
		yield self.parentheses;
		yield self.words;
		yield self.length;
		yield self.cleaned_lines;
		yield self.cleaned_spaces;
		yield self.cleaned_tabs;
		yield self.cleaned_words;
		yield self.cleaned_length;
		yield self.http_total_links;
		yield self.other_total_links;
		yield self.domain_com;
		yield self.domain_edu;
		yield self.domain_gov;
		yield self.domain_org;
		yield self.domain_net;
		yield self.http_pruned_links;
		yield self.sponsored_links;
		yield self.nonsponsored_links;
		yield self.unknown_links;
		yield self.title_length;
		yield self.javascript_scripts_count;
		yield self.javascript_length;
		yield self.img_count;
		yield self.meta_length;
		yield self.meta_keywords;
		yield self.meta_description_length;
		yield self.meta_redirect;
		yield self.iframe_count;
		yield self.word_ad;
		yield self.word_sponsored;
		yield self.word_advertisement;
		yield self.word_money;
		yield self.word_cash;
		yield self.word_prize;
		yield self.word_buy;
		yield self.word_sell;
		yield self.word_cookie;
		yield self.word_redirect;
		yield self.word_free;
		yield self.word_affiliate;
		yield self.word_banner;
		yield self.word_scentsy;
		yield self.word_adword;
		yield self.word_marketing;
		yield self.word_track;
		yield self.robot;
		yield self.XMLHttpRequest_count;
		yield self.js_redirect;
		yield self.ajax;
		yield self.document_write;
		yield self.url_behance;
		yield self.url_incapsula;
		yield self.url_godaddy_park;
		yield self.url_amazon;


def create_data(filepath):
	urls = set() #this way we don't have duplicate URLs
	filename = os.path.basename(filepath)
	with open(filepath, 'rb') as infile:
		text = infile.read()

	values = ResultFileEntry(filename)
	if filename in train_keys:
		values.sponsored = train_keys[filename]
	values.lines = text.count('\n')
	values.spaces = text.count(' ')
	values.tabs = text.count('\t')
	values.braces = text.count('{')
	values.brackets = text.count('[')
	values.parentheses = text.count('(')
	values.words = len(re.split('\s+', text))
	values.length = len(text)
	
	values.url_behance = text.count('behance.net')
	values.url_incapsula = text.count('incapsula.com')
	values.url_godaddy_park = text.count('mcc.godaddy.com/park/')
	values.url_amazon = text.count('rcm.amazon.com')

	#use lxml parser for faster speed
	parsed = BeautifulSoup(text, "lxml")
	cleaned = parsed.getText()
	values.cleaned_lines = cleaned.count('\n')
	values.cleaned_spaces = cleaned.count(' ')
	values.cleaned_tabs = cleaned.count('\t')
	values.cleaned_words = len(re.split('\s+', cleaned))
	values.cleaned_length = len(cleaned)

	for anchor in parsed.findAll('a', href=True):
		if anchor['href'].startswith("http"):
			#count of different generic top level domains (.com, .edu, .gov, etc.)
			try:
				res = get_tld(anchor['href'], as_object=True, fail_silently=True)
				if res:
					if res.suffix == 'com':
						values.domain_com += 1
					elif res.suffix == 'edu':
						values.domain_edu += 1
					elif res.suffix == 'gov':
						values.domain_gov += 1
					elif res.suffix == 'org':
						values.domain_org += 1
					elif res.suffix == 'net':
						values.domain_net += 1
						
					values.http_total_links += 1
					#very important to use str(res) here, otherwise it adds the object res and not the string
					#resulting in a huge number of urls to parse (and store in memory!)
					urls.add(str(res))
			except ValueError:
				print("IPv6 URL?")
		else:
			values.other_total_links += 1

	values.http_pruned_links = len(urls)

	if filename in train_keys:
		if train_keys[filename] == 1:
			values.sponsored_links = list(urls)
		elif train_keys[filename] == 0:
			values.nonsponsored_links = list(urls)
	else:
		values.unknown_links = list(urls)

	if parsed.title and parsed.title.string:
		values.title_length = len(parsed.title.string)
		
	javascript = parsed.findAll('script')
	values.javascript_scripts_count = len(javascript)
	for j in javascript:
		values.javascript_length += len(j)

	values.img_count = len(parsed.findAll('img'))
	
	for meta in parsed.findAll('meta'):
		if meta.has_attr('content'):
			values.meta_length += len(meta['content'])
	
	for meta in parsed.findAll('meta', attrs={"name":"keywords"}):
		if meta.has_attr('content'):
			values.meta_keywords += len(meta['content'].split(","))
	
	for meta in parsed.findAll('meta', attrs={"name":"description"}):
		if meta.has_attr('content'):
			values.meta_description_length += len(meta['content'])
			
	for meta in parsed.findAll('meta', attrs={"name":"robots"}):
		if meta.has_attr('content'):
			values.robot = 1
	
	#http://webmaster.iu.edu/tools-and-guides/maintenance/redirect-meta-refresh.phtml
	for meta in parsed.findAll('meta', attrs={"http-equiv":"refresh"}):
		if meta.has_attr('content'):
			if "URL" in meta['content']:
				values.meta_redirect = 1
			
	for iframe in parsed.findAll('iframe'):
		if iframe.has_attr('src'):
			values.iframe_count += 1

	values.word_ad = len(re.findall("\sad[s]*", cleaned.lower())) #ad or ads
	values.word_sponsored = len(re.findall("\ssponsor[ed]*", cleaned.lower())) #sponsor or sponsored
	values.word_advertisement = len(re.findall("\sadvertise[ment]*", cleaned.lower())) #advertise or advertisement
	values.word_money = len(re.findall("\smoney", cleaned.lower())) #money
	values.word_cash = len(re.findall("\scash", cleaned.lower())) #cash
	values.word_prize = len(re.findall("\sprize", cleaned.lower())) #prize
	values.word_buy = len(re.findall("\sbuy", cleaned.lower())) #buy
	values.word_sell = len(re.findall("\ssell", cleaned.lower())) #sell
	values.word_cookie = len(re.findall("\scookie[s]*", text.lower())) #cookie or cookies
	values.word_redirect = len(re.findall("\sredirect[ed]*", text.lower())) #redirect or redirected
	values.word_free = len(re.findall("\sfree", text.lower())) #free
	values.word_affiliate = len(re.findall("\saffiliate[d]*", text.lower())) #affiliate or affiliated
	values.word_banner = len(re.findall("\sbanner[s]*", text.lower())) #banner or banners
	values.word_scentsy = len(re.findall("\sscentsy", text.lower())) #scentsy
	values.word_adword = len(re.findall("\sadword[s]*", text.lower())) #adword or adwords
	values.word_marketing = len(re.findall("\smarketing", text.lower())) #marketing
	values.word_track = len(re.findall("\strack[ing]*", text.lower())) #track or tracking
	values.XMLHttpRequest_count = len(re.findall("xmlhttprequest", text.lower())) #XMLHttpRequest
	values.js_redirect = len(re.findall("window\.location|window\.location\.href|window\.location\.assign|window\.location\.replace|window\.location\.host|window\.top\.location", text.lower())) #different possibilites for javascript redirects
	values.ajax = len(re.findall("\$\.ajax\(\{", text.lower())) #ajax call: $.ajax({
	values.document_write = len(re.findall("document.write", text.lower())) #document.write
	
	return values




print('--- Read training labels')
train = pd.read_csv('./data/train_v2.csv')
train_keys = dict([a[1] for a in train.iterrows()])
del train #free up this memory?
	
	
filepaths = glob.glob('data/*/*.txt')

#random.shuffle(filepaths)
#filepaths = filepaths[0:1000]


num_tasks = len(filepaths)


bar = ProgressBar(num_tasks, max_width=40)
p = multiprocessing.Pool()
#imap_unordered means we don't care about the order of the returned results
results = p.imap_unordered(create_data, filepaths) #chunksize default = 1
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


column_names = ["file", "sponsored", "lines", "spaces", "tabs", "braces", "brackets", "parentheses", "words", "length", "cleaned_lines",
				"cleaned_spaces", "cleaned_tabs", "cleaned_words", "cleaned_length", "http_total_links", "other_total_links",
				"domain_com", "domain_edu", "domain_gov", "domain_org", "domain_net", "http_pruned_links", "sponsored_links",
				"nonsponsored_links", "unknown_links", "title_length", "javascript_scripts_count", "javascript_length",
				"img_count", "meta_length", "meta_keywords", "meta_description_length", "meta_redirect", "iframe_count", "word_ad", "word_sponsored",
				"word_advertisement", "word_money", "word_cash", "word_prize", "word_buy", "word_sell", "word_cookie", "word_redirect",
				"word_free", "word_affiliate", "word_banner", "word_scentsy", "word_adword", "word_marketing", "word_track", "robot",
				"XMLHttpRequest_count", "js_redirect", "ajax", "document_write", "url_behance", "url_incapsula", "url_godaddy_park",
				"url_amazon"]


df_full = pd.DataFrame(list(results), columns=column_names)

with pd.option_context('display.max_rows', 20, 'display.max_columns', len(column_names)):
	print(df_full)

#create correlation table
cor = df_full.corr(method='pearson')
cor = np.round(cor, decimals=2) #round to 2 decimal places
cor.to_csv('correlations.csv')

#print(df_full.columns.values)


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

