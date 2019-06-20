import sys
from pyspark import SparkContext

def main():
    sc = SparkContext("local[3]", "word count")
    sc.setLogLevel("ERROR")
    lines = sc.textFile("data/word_count.text")
    words = lines.flatMap(lambda line:line.split(" "))

    wordCount = words.countByValue()

    for word,count in wordCount.items():
        print(word,count)

if __name__ == "__main__":
    main()
