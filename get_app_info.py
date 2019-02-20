from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType, DoubleType, DateType
import pyspark.sql.functions as f
import requests, json, os, sys, time, re

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

def get_app_detail(appid):
    for i in range(3):
        try:
            r = requests.get('http://store.steampowered.com/api/appdetails?appids=' + str(appid)).json()
            if r.get(appid).get('success'):
                time.sleep(0.5)
                detail = r.get(appid).get('data')
                return detail
        except:
            time.sleep(5)
            pass


def get_app_info():
    for i in range(3):
        try: 
            r = requests.get('https://api.steampowered.com/ISteamApps/GetAppList/v2/').json()
            apps = r.get('applist').get('apps')
            appid = sc.parallelize(apps).map(lambda x : x.get('appid'))
            app_detail = appid.map(lambda x : get_app_detail(str(x))).filter(lambda x : x).cache()
            return app_detail
        except:
            time.sleep(5)
