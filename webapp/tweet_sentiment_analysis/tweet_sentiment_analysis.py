import re

from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, HashingTF
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)


def train_filter_func(row):
    row_dict = row.asDict()
    val = row_dict['sentiment']
    if val in ['0', '4']:
        return True
    else:
        return False


def int_cast(row):
    row_dict = row.asDict()
    val = int(row_dict['sentiment'])
    if val == 0:
        row_dict['sentiment'] = 0
    else:
        row_dict['sentiment'] = 1
    return row_dict


def _clean_tweet_text(text):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split())


def clean_train_tweet(row):
    '''
    Function to clean tweet text by removing links, special characters using regex .
    :param tweet_text: text of tweet recieved from database
    :return: cleaned text of tweet
    '''
    row['tweet_text'] = _clean_tweet_text(row['tweet_text'])
    return row


def clean_test_tweet(row):
    '''
    Function to clean tweet text by removing links, special characters using regex .
    :param tweet_text: text of tweet recieved from database
    :return: cleaned text of tweet
    '''
    row = row.asDict()
    row['tweet_text'] = _clean_tweet_text(row['tweet_text'])
    return row


################# Training data from Sentiment140  #####################################################################
train_df = spark.read.csv("hdfs:///user/maria_dev/sent_140.csv")
train_df = train_df.selectExpr("_c0 as sentiment", "_c5 as tweet_text").select('sentiment', 'tweet_text')
train_rdd = train_df.rdd
train_rdd = train_rdd.filter(train_filter_func).map(int_cast)
train_rdd = train_rdd.map(clean_train_tweet)
clean_train_df = spark.createDataFrame(train_rdd, schema="tweet_text: string, sentiment: int")

############### Extracting orc datafile stored from HDFS ###############################################################
tweet_df = spark.read.orc('hdfs:///user/maria_dev/tweet_usermention')
tweet_df = tweet_df.select('tweet_text')
tweet_rdd = tweet_df.rdd
clean_tweet_rdd = tweet_rdd.map(clean_test_tweet)
clean_tweet_df = spark.createDataFrame(clean_tweet_rdd, "tweet_text: string")
clean_tweet_df = clean_tweet_df.dropDuplicates()

######## Feature Transformation #######################################################################################

############ Tokenize ################################################################################################

tokenizer = Tokenizer(inputCol="tweet_text", outputCol="words")

############# Stop Words Remover ######################################################################################
remover = StopWordsRemover(inputCol="words", outputCol="filtered")

############ CountVectorizer #########################################################################################
cv = CountVectorizer(inputCol="filtered", outputCol="cvfeatures", minDF=2.0)

hashtf = HashingTF(numFeatures=2 ** 16, inputCol="words", outputCol='tffeatures')

#############Feature Extractors #####################################################################################

############# IDF ####################################################################################################
idf = IDF(inputCol='cvfeatures', outputCol="features",
          minDocFreq=5)  # minDocFreq: remove sparse terms # it down-weights
# columns which appear frequently in a corpus.
idf2 = IDF(inputCol='tffeatures', outputCol="features", minDocFreq=5)
############# Logistic Regression ######################################################################################
lr = LogisticRegression(labelCol="sentiment")
nb = NaiveBayes(labelCol="sentiment")

############# Pipelines ######################################################################################
pipeline1 = Pipeline(stages=[tokenizer, remover, cv, idf, lr])  # CountVectorizer + IDF + Logistic Regression
pipelineFit1 = pipeline1.fit(clean_train_df)
predictions = pipelineFit1.transform(clean_tweet_df)

pipeline2 = Pipeline(stages=[tokenizer, remover, hashtf, idf2, lr])  # HashingTF + IDF + Logistic Regression
pipelineFit2 = pipeline2.fit(clean_train_df)
predictions2 = pipelineFit2.transform(clean_tweet_df)

pipeline_nb = Pipeline(stages=[tokenizer, remover, hashtf, idf2, nb])  # HashingTF + IDF + Naive Bayes
pipelineFit_nb = pipeline_nb.fit(clean_train_df)
predictionsnb = pipelineFit_nb.transform(clean_tweet_df)
