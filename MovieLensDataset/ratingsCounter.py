from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsCounter")

sc = SparkContext(conf = conf)

lines = sc.textFile("data/movie.data")
ratings = lines.map(lambda line:line.split()[2])
result = ratings.countByValue()

for rating in sorted(result.keys()):
    print(rating, result[rating])