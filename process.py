import re, os, sys
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import numpy as np

#337304 total HTML files
process_folders = ["./data/0/", "./data/1/", "./data/2/", "./data/3/", "./data/4/"]

def stripHTMLandClean(text):
	#use lxml parser for faster speed
	cleaned = BeautifulSoup(text, "lxml").get_text() #just text, not HTML
	#TODO: punctuation, stop words, numbers
	cleaned = cleaned.lower()
	return cleaned

#use Pandas for reading file
train = pd.read_csv("./data/train.csv", header=0, delimiter=",", quoting=3)

cleaned_training = []
trained_ordered = pd.DataFrame(columns=['file', 'sponsored'])

print "Starting processing..."

for folder in process_folders:
	onlyfiles = []
	for root, dirs, files in os.walk(os.path.abspath(folder)):
		for file in files:
			onlyfiles.append(os.path.join(root, file))
	for openfile in onlyfiles:
		sponsored = train.loc[train['file'] == os.path.basename(openfile)]
		if not sponsored.empty:
			f = open(openfile, 'r')
			contents = f.read()
			cleaned = stripHTMLandClean(contents)
			cleaned_training.append(cleaned)
			series = pd.Series([str(os.path.basename(openfile)), sponsored.iloc[0,1]], index=['file', 'sponsored'])
			trained_ordered = trained_ordered.append(series, ignore_index=True)
			f.close()

print trained_ordered

print "Finished reading training HTML data."

vectorizer = CountVectorizer(analyzer = "word",
			tokenizer = None,
			preprocessor = None,
			stop_words = None,
			max_features = 5000)
			
print "Vectorization done.  Fitting data..."

# fit_transform() does two functions: First, it fits the model
# and learns the vocabulary; second, it transforms our training data
# into feature vectors. The input to fit_transform should be a list of 
# strings.
train_data_features = vectorizer.fit_transform(cleaned_training)
# Numpy arrays are easy to work with, so convert the result to an 
# array
train_data_features = train_data_features.toarray()
#print "Trained data set dimensions: ", train_data_features.shape
# Take a look at the words in the vocabulary
vocab = vectorizer.get_feature_names()
#print vocab

# Sum up the counts of each vocabulary word
dist = np.sum(train_data_features, axis=0)

# For each, print the vocabulary word and the number of times it 
# appears in the training set
#for tag, count in zip(vocab, dist):
	#print count, tag

# Initialize a Random Forest classifier with 100 trees
forest = RandomForestClassifier(n_estimators = 100, n_jobs=4)

# Fit the forest to the training set, using the bag of words as 
# features and the sentiment labels as the response variable
#
# This may take a few minutes to run
forest = forest.fit( train_data_features, trained_ordered["sponsored"] )


test = pd.read_csv("./data/sampleSubmission.csv", header=0, delimiter=",", quoting=3)

print "Test dimensions: ", test.shape

clean_test_data = []
for i in xrange(0, len(test)):
	for folder in process_folders:
		checkfile = os.path.join(folder, test['file'][i])
		if os.path.isfile(checkfile):
			f = open(checkfile, 'r')
			contents = f.read()
			cleaned = stripHTMLandClean(contents)
			clean_test_data.append(cleaned)
			f.close()
			break

# Get a bag of words for the test set, and convert to a numpy array
test_data_features = vectorizer.transform(clean_test_data).toarray()

#print "Test data features: ", test_data_features

# Use the random forest to make sentiment label predictions
result = forest.predict(test_data_features)

#print "Result: ", result

# Copy the results to a pandas dataframe with a "file" and "sponsored" column
output = pd.DataFrame( data={"file":test['file'], "sponsored":result} )

# Use pandas to write the comma-separated output file
output.to_csv("Bag_of_Words_model.csv", index=False, quoting=3)
