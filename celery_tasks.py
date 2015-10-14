from celery import Celery
from bs4 import BeautifulSoup
from urlparse import urlparse
import re, zipfile, os

app = Celery('tasks', broker='amqp://guest@192.168.1.140//', backend='redis://192.168.1.140')

@app.task
def processFile(filename, sponsored):
	process_zips = ["./data/0.zip", "./data/1.zip", "./data/2.zip", "./data/3.zip", "./data/4.zip", "./data/5.zip"]
	values = {}
	
	for zip_file in process_zips:
		archive = zipfile.ZipFile(zip_file, 'r')
		file_paths = zipfile.ZipFile.namelist(archive)
		for file_path in file_paths:
			#we have to do this because file_paths also includes the folder, e.g. 0/filename.txt
			if str(file_path).endswith(filename):
				text = archive.read(file_path)
				
				urls = []
				values['file'] = filename
				if sponsored != 2:
					values['sponsored'] = sponsored
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
							urls.append(parsedurl)
						except ValueError:
							print("IPv6 URL?")
					else:
						values['other_total_links'] += 1
				
				#remove duplicate URLS (this will change the order)
				urls = list(set(urls))
				values['http_pruned_links'] = len(urls)
				
				values['sponsored_links'] = []
				values['nonsponsored_links'] = []
				if sponsored != 2: #we're in the training set
					for url in urls:
						if sponsored == 1:
							values['sponsored_links'].append(url)
						elif sponsored == 0:
							values['nonsponsored_links'].append(url)
				
				if parsed.title and parsed.title.string:
					values['title_length'] = len(parsed.title.string)
				
				del values['sponsored_links']
				del values['nonsponsored_links']
				return values #return right away, skip the rest of the for loops
	
	return values
