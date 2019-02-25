from pyspark.ml.recommendation import ALS
from pyspark.sql.window import Window
import pyspark.sql.functions as f

def als_model():

	user_inventory = spark.sql("SELECT * FROM userinfo").filter('playtime_forever > 0')
	ratings = user_inventory.withColumn("user", f.dense_rank().over(Window.orderBy("userid")))
	correspond = ratings.select('userid', 'user').dropDuplicates()

	als = ALS(userCol = "user", itemCol = "appid", ratingCol = "playtime_forever")
	model = als.fit(ratings)
	model.save("als_model")
	top20 = model.recommendForAllUsers(20)

	recommend = top20.join(correspond, top20.user == correspond.user).select('userid', 'recommendations')
	recommendList = recommend.rdd.map(lambda x : (x[0], [appid[0] for appid in x[1]])).collect()

	for r in recommendList:
	    userid = r[0]
	    idList = r[1]
	    spark.sql("INSERT INTO als_top20 ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',\
												'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
										 (userid, idList[0], idList[1], idList[2], idList[3], idList[4], idList[5], \
												  idList[6], idList[7], idList[8], idList[9], idList[10], \
												  idList[11], idList[12], idList[13], idList[14], idList[15], \
												  idList[16], idList[17], idList[18], idList[19]))