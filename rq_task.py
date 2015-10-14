
from bs4 import BeautifulSoup
from urlparse import urlparse
import re, os



def processFile(filepath, sponsored):
	values = {}
	urls = []
	filename = os.path.basename(filepath)
	with open(filepath, 'rb') as infile:
		text = infile.read()

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
	values['unknown_links'] = []
	if sponsored != 2:
		for url in urls:
			if sponsored == 1:
				values['sponsored_links'].append(url)
			elif sponsored == 0:
				values['nonsponsored_links'].append(url)
	else:
		values['unknown_links'] = urls

	if parsed.title and parsed.title.string:
		values['title_length'] = len(parsed.title.string)
		
	javascript = parsed.findAll('script')
	values['javascript_scripts_count'] = len(javascript)
	values['javascript_length'] = 0
	for j in javascript:
		values['javascript_length'] += len(j)
		
	values['img_count'] = len(parsed.findAll('img'))
	
	return values	
