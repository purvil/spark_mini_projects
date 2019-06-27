from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)


def main():
    names = sc.textFile("data/marvel-names.txt")
    


if __name__ == "__main__":
    main()