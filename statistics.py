import pandas as pd
import numpy as np


train = pd.read_csv("./data/train_v2.csv", header=0, delimiter=",", quoting=3)
sample = pd.read_csv("./data/sampleSubmission_v2.csv", header=0, delimiter=",", quoting=3)


sponsoredDict = pickle.load(open( "sponsoredDict.p", "rb")) #key: website, value: filenames
notsponsoredDict = pickle.load(open( "notsponsoredDict.p", "rb")) #key: website, value: filenames
sampleDict = pickle.load(open( "sampleDict.p", "rb")) #key: filename, value: websites


