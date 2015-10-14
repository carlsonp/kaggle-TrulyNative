## Kaggle Truly Native

#### Intro

This is my set of scripts for the
[Kaggle](http://www.kaggle.com) Truly Native
competition.  This was my first Kaggle competition
and it was a great learning experience.  The task
was to look through over 300,000 HTML files from
StumbleUpon to identify if a website did or did
not have
[native advertising](https://en.wikipedia.org/wiki/Native_advertising)
in it.  The data consisted of a training set with known
sponsored values (0 or 1) and a testing set for prediction.
This testing set was then submitted via Kaggle for the competition.
The competition used an
[Area Under the Curve (AUC)](https://en.wikipedia.org/wiki/Receiver_operating_characteristic)
score for evaluation.  My final AUC score was 0.94858.

I ended up trying a lot of different technologies for the processing
side given the large dataset in an attempt to speed
up the identification of features for the machine
learning portion.  The initial multiprocessing
approach took a few hours to run so I looked into
alternatives.  Here's what I tried:

* [Celery](http://www.celeryproject.org/) is a distributed
task queuing system.
* [RQ](http://python-rq.org/) is another lighter weight
distributed task queuing system.

Unfortunately, both systems didn't seem to work under
the sheer number of files to process.  I'm not sure if this
was a problem with the configuration and setup or the message
broker [RabbitMQ](https://www.rabbitmq.com/) that I was using.

For machine learning I used the Python library
[scikit](http://scikit-learn.org) and Random Forests.
I heavily modified an example script from the Kaggle
forums and put in improved and updated features, decreased
memory usage via Python __slots__, and used stacking
to combine multiple Random Forest predictions into a single
improved prediction.

#### How to run

* Run `btb_native_basic4.py`, this will save a serialized
copy of the Pandas DataFrame to to file.
* Run `scikit_generate_prediction2.py` to generate
a single submission.
* For a slight improvement, use stacking and the script
`scikit_generate_prediction_stacking.py`.

#### Licenses

This code is under GPLv3.

The [fastdupes](https://github.com/ssokolow/fastdupes)
code is under GPLv2.
