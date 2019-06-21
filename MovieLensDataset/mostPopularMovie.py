from pyspark import SparkContext, SparkConf

def loadMovieNames():
    movieNames = {}
    with open("data/movie.item", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[fields[0]] = fields[1]
    return movieNames

def main():
    conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
    sc = SparkContext(conf = conf)
    
    nameDict = sc.broadcast(loadMovieNames())
    movieData = sc.textFile("data/movie.data")
    
    totalMovieOccurence = movieData.map(lambda line : (line.split()[1], 1)).reduceByKey(lambda x, y : x + y)
    
    totalMovieOccurenceSorted = totalMovieOccurence.map(lambda x : (x[1], x[0])).sortByKey().map(lambda x : (nameDict.value[x[1]], x[0]))
    totalMovieOccurenceSorted.saveAsTextFile("out/mostPoupular")
    
if __name__ == "__main__":
    main()
    
    