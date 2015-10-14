from __future__ import print_function
import pickle, os, sys, glob, hashlib

from sklearn.ensemble import GradientBoostingClassifier
import pandas as pd
#https://github.com/ssokolow/fastdupes
import fastdupes

test_files = set(pd.read_csv('./data/sampleSubmission_v2.csv').file.values)
train = pd.read_csv('./data/train_v2.csv')


df_full = pickle.load(open( "df_full.p", "rb"))


#no point using empty files in our training set so we remove them
print('--- Removing empty files')
filepaths = glob.glob('data/*/*.txt')
for filepath in filepaths:
	if os.path.getsize(filepath) == 0:
		filename = os.path.basename(filepath)
		df_full = df_full[df_full.file != filename]
		if filename in test_files:
			print("Found empty file in submission: ", filename)



print('--- Training gradient')
clf = GradientBoostingClassifier(n_estimators=200, random_state=0, verbose=2)
train_data = df_full[df_full.sponsored.notnull()].fillna(0)
test = df_full[df_full.sponsored.isnull() & df_full.file.isin(test_files)].fillna(0)
clf.fit(train_data.drop(['file', 'sponsored'], 1), train_data.sponsored)

#normalized value between 0 and 1
feature_importances = pd.Series(clf.feature_importances_, index=train_data.drop(['file', 'sponsored'], 1).columns)
feature_importances.sort()
print(feature_importances)


print('--- Create predictions and submission')
submission = test[['file']].reset_index(drop=True)
submission['sponsored'] = clf.predict_proba(test.drop(['file', 'sponsored'], 1))[:, 1]


#if we have duplicate files in the testing set that are in the training set,
#there's no reason to use our prediction, just use the true value!
print("--- Finding duplicate files")
duplicates = 0
dupes = fastdupes.find_dupes(filepaths, exact=True)

for sets in dupes:
	counter = 0
	total = 0
	ratio = None #cached sponsored ratio calculation
	for f in dupes[sets]:
		filename = os.path.basename(f)
		if filename in test_files:
			if ratio is None:
				#search through same set to find all files in the training set and sum up sponsored totals and increment a counter
				#this is needed because there are some instances where there are conflicting reports about duplicate files being
				#sponsored or not sponsored, thus we just take an average
				for k in dupes[sets]:
					past_filename = os.path.basename(k)
					if past_filename in train['file'].values:
						total += train.loc[train['file'] == past_filename, 'sponsored'].values[0]
						counter += 1
				
				if total == 0:
					ratio = 0
				else:
					ratio = float(total) / float(counter)
			if ratio is not None:
				#average our initial prediction with the calculated ratio
				combined_ratio = (submission.loc[submission['file'] == filename, 'sponsored'].values[0] + ratio) / 2
				submission.loc[submission['file'] == filename, 'sponsored'] = combined_ratio
				duplicates += 1

print("Found: ", duplicates, " duplicate files in the testing set.")

#make sure submission has the correct number of rows
if len(submission) != 66772:
	print("Error: wrong dimension! Not generating submission CSV file.")
else:
	submission.to_csv('native_btb_basic_submission.csv', index=False)
