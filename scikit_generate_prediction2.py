from __future__ import print_function
import pickle, os, sys, glob, hashlib

from sklearn.ensemble import RandomForestClassifier
import pandas as pd

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



#https://www.youtube.com/watch?v=0GrciaGYzV0
print('--- Training random forest')
clf = RandomForestClassifier(n_estimators=300, n_jobs=-1, random_state=0)
train_data = df_full[df_full.sponsored.notnull()].fillna(0)
test = df_full[df_full.sponsored.isnull() & df_full.file.isin(test_files)].fillna(0)
clf.fit(train_data.drop(['file', 'sponsored'], 1), train_data.sponsored)

#normalized value between 0 and 1
feature_importances = pd.Series(clf.feature_importances_, index=train_data.drop(['file', 'sponsored'], 1).columns)
feature_importances.sort()
with pd.option_context('display.max_rows', len(feature_importances), 'display.max_columns', 10):
	print(feature_importances)


print('--- Create predictions and submission')
submission = test[['file']].reset_index(drop=True)
submission['sponsored'] = clf.predict_proba(test.drop(['file', 'sponsored'], 1))[:, 1]


#make sure submission has the correct number of rows
if len(submission) != 66772:
	print("Error: wrong dimension! Not generating submission CSV file.")
else:
	submission.to_csv('native_btb_basic_submission.csv', index=False)
