from pyspark import SparkConf, SparkContext
import re


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


def main():
    conf = SparkConf().setMaster("local").setAppName("WordCount")
    sc = SparkContext(conf = conf)

    book = sc.textFile("data/Book.txt")
    words = book.flatMap(normalizeWords)

    wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
    wordCountsSorted.map(lambda x:(x[1], x[0])).saveAsTextFile("out/sortedWordCount.txt")
    
if __name__ == "__main__":
    main()


