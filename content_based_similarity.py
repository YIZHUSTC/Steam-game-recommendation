from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType, DoubleType, DateType
from pyspark.ml.feature import StopWordsRemover, HashingTF, IDF, Tokenizer
from pyspark.ml.linalg import Vectors, SparseVector
import pyspark.sql.functions as f

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

def content_similarity(parsed):
    content_df = parsed.toDF().select('appid', \
               f.concat('about_the_game', 'detailed_description', 'short_description').alias('content'))\
                .filter("content != ''")

    tokenizer = Tokenizer(inputCol = 'content', outputCol = 'words')
    words = tokenizer.transform(content_df)

    remover = StopWordsRemover(inputCol = 'words', outputCol = 'filtered')
    filtered = remover.transform(words)

    hashingTF = HashingTF(inputCol = 'filtered', outputCol = 'rawFeatures')
    featurized = hashingTF.transform(filtered)

    idf = IDF(inputCol = 'rawFeatures', outputCol = 'features')
    idfModel = idf.fit(featurized)
    rescaled = idfModel.transform(featurized)
    
    app = rescaled.select('appid', 'features')
    app_feature = app.join(app.alias('app2'), app.appid != app2.appid)\
                           .toDF('appid1', 'features1', 'appid2', 'features2')
    cosine_similarity = app_feature.rdd.map(lambda row: (row.appid1, row.features1, row.appid2, row.features2))\
                    .map(lambda x : {'appid1': x[0], 'appid2': x[2], 
                                     'similarity': float(x[1].dot(x[3]) / (x[1].norm(2) * x[3].norm(2)))
                                                }).toDF()
    
    cosine_similarity.write.parquet('app_content_based_similarity', mode = 'overwrite')
    cosine_similarity.write.saveAsTable('content_similarity')
    
    applist = cosine_similarity.select('appid1').distinct().rdd.map(lambda row:row[0]).collect()
    for appid in applist:
        top20 = findTop20(cosine_similarity, appid)
        spark.sql("INSERT INTO content_top20 ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',\
                                                    '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                                              (appid, top20[0], top20[1], top20[2], top20[3], top20[4], top20[5], \
                                                      top20[6], top20[7], top20[8], top20[9], top20[10], top20[11], \
                                                      top20[12], top20[13], top20[14], top20[15], top20[16], \
                                                      top20[17], top20[18], top20[19]))
    return cosine_similarity


def findTop20(similarity_df, appid):
    top20 = similarity_df.filter(similarity_df.appid1 == appid).sort('similarity', ascending = False)\
            .select('appid2').limit(20)
    return top20.rdd.map(lambda row:row[0]).collect()
