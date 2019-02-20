from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType, DoubleType, DateType
from pyspark.ml.feature import StopWordsRemover, HashingTF, IDF, Tokenizer
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.mllib.feature import Normalizer
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

    single = rescaled.rdd.map(lambda row: (row.appid, row.features))
    pairwise = single.cartesian(single)
    cosine_similarity = pairwise.map(lambda x : {'appid1': x[0][0], 'appid2': x[1][0], 
                            'similarity': float(x[0][1].dot(x[1][1]) / (x[0][1].norm(2) * x[1][1].norm(2)))
                                                })
    cosine_similarity_df = spark.createDataFrame(cosine_similarity)
    cosine_similarity_df.write.parquet('app_similarity', mode = 'overwrite')
    
    return cosine_similarity_df