from wordcloud import WordCloud
import matplotlib.pyplot as plt
from django.conf import settings
#input from cassandra

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
auth_provider = PlainTextAuthProvider(username=settings.CASSANDRA_USERNAME,
                                          password=settings.CASSANDRA_PASSWORD)  # authentication
cluster = Cluster(settings.CASSANDRA_HOST, executor_threads=10, auth_provider=auth_provider)
session = cluster.connect("twitter_data")
res = session.execute("select tweet_hashtag, count(*) as num_count from twitter_data.test_usermention where "
                      "tweet_usermention='google' and tweet_timestamp >= '2018-11-22' and tweet_timestamp "
                      "<= '2018-11-24' group by tweet_hashtag allow filtering;")
base_str = ""
for row in res:
    base_str = base_str + f'{row.tweet_hashtag} ' * row.num_count

wordcloud = WordCloud(width=1600, height=800,max_font_size=200).generate(ss)
plt.figure(figsize=(12,10))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
