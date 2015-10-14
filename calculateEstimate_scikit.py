from __future__ import print_function
import re, os, sys, zipfile, pickle
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import numpy as np
from collections import defaultdict
#https://pypi.python.org/pypi/etaprogress/
from etaprogress.progress import ProgressBar


#use Pandas for reading file
train = pd.read_csv("./data/train.csv", header=0, delimiter=",", quoting=3)
sample = pd.read_csv("./data/sampleSubmission.csv", header=0, delimiter=",", quoting=3)

sponsoredDict = pickle.load(open( "sponsoredDict.p", "rb")) #key: website, value: filenames
notsponsoredDict = pickle.load(open( "notsponsoredDict.p", "rb")) #key: website, value: filenames
sampleDict = pickle.load(open( "sampleDict.p", "rb")) #key: filename, value: websites

sponsoredDict_reordered = defaultdict(list)
notsponsoredDict_reordered = defaultdict(list)

for website, filenames in sponsoredDict.items():
	for f in filenames:
		sponsoredDict_reordered[f].append(website)
for website, filenames in notsponsoredDict.items():
	for f in filenames:
		notsponsoredDict_reordered[f].append(website)


bar = ProgressBar(len(train), max_width=40)
train_data = []
for index, row in train.iterrows():
	website_string = ' '.join(sponsoredDict_reordered[row['file']]) + ' '.join(notsponsoredDict_reordered[row['file']])
	bar.numerator = index
	print(bar, end='\r')
	sys.stdout.flush()
	train_data.append(website_string)
print()

bar = ProgressBar(len(sample), max_width=40)
sample_data = []
for index, row in sample.iterrows():
	website_string = ' '.join(sampleDict[row['file']])
	bar.numerator = index
	print(bar, end='\r')
	sys.stdout.flush()
	sample_data.append(website_string)
print()

print("Finished getting data ready.")
	
#http://www.vikparuchuri.com/blog/natural-language-processing-tutorial/
#https://www.kaggle.com/c/word2vec-nlp-tutorial/details/part-1-for-beginners-bag-of-words
#http://scikit-learn.org/stable/modules/feature_extraction.html

#http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html
vectorizer = CountVectorizer(analyzer = "word",
			tokenizer = None,
			preprocessor = None,
			stop_words = None,
			max_features = 800) #TODO: check memory limit here

# fit_transform() does two functions: First, it fits the model
# and learns the vocabulary; second, it transforms our training data
# into feature vectors. The input to fit_transform should be a list of 
# strings.
train_data_features = vectorizer.fit_transform(train_data).toarray()

print(train_data_features.shape)


#http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html

# Initialize a Random Forest classifier with trees
# use as many CPU cores as possible (n_jobs=-1)
forest = RandomForestClassifier(n_estimators = 1000, n_jobs=-1, verbose=2) 

# Fit the forest to the training set, using the bag of words as 
# features and the sentiment labels as the response variable
#
# This may take a few minutes to run
forest = forest.fit(train_data_features, train["sponsored"])


#Run the prediction...

# Get a bag of words for the test set, and convert to a numpy array
test_data_features = vectorizer.transform(sample_data).toarray()

print(test_data_features.shape)

# Use the random forest to make predictions
result = forest.predict(test_data_features)

# Copy the results to a pandas dataframe
output = pd.DataFrame(data={"file":sample["file"], "sponsored":result})

# Use pandas to write the comma-separated output file
output.to_csv("SubmissionEstimate_RF.csv", header=True, index=False, sep=',')
