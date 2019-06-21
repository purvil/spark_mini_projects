from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


def main():
    conf = SparkConf().setMaster("local").setAppName("friendsByAge")
    sc = SparkContext(conf = conf)

    friends = sc.textFile("data/fakefriends.csv")

    rdd = friends.map(parseLine)
    
    totalByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1]))
    averageByAge = totalByAge.mapValues(lambda x: int(x[0] / x[1]))
                                                            
    for result in averageByAge.collect():
        print(result)

if __name__ == "__main__":
    main()
