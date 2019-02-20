from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType, DoubleType, DateType
import pyspark.sql.functions as f
import requests, json, os, sys, time, re

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

def get_game_list(user_id):
    base_url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/'
    keys = [key1, key2]
    curr_keyid = 0
    params = {
            'key' : keys[curr_keyid],
            'steamid' : user_id.strip(),
            'format' : 'json' }
    
    games = None
    for i in range(3):
        try:
            games = requests.get(base_url, params = params).json().get('response').get('games')
            break
        except:
            try:
                curr_keyid = 1 - curr_keyid
                params.update({'key' : keys[curr_keyid]})
                games = requests.get(base_url, params = params).json().get('response').get('games')
                break
            except:
                time.sleep(5)
                pass
    
    if games and len(games) > 0:
        gamelist = []
        f = open('userinfo', 'a')
        for g in games:
            gamelist.append(g)
            g.update({'userid' : int(user_id.strip())})
            f.write(json.dumps(g))
            f.write('\n')
            userid = int(user_id.strip())
            appid = g.get('appid')
            playtime_forever = g.get('playtime_forever')
            spark.sql("INSERT INTO userinfo ('%s', '%s', '%s')" % (userid, appid, playtime_forever))
        return gamelist
