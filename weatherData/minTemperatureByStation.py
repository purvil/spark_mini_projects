from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationID, entryType, temperature)

def main():
    conf = SparkConf().setMaster("local").setAppName("minTemperature")
    sc = SparkContext(conf = conf)
    
    weather = sc.textFile("data/1800.csv")
    cleanData = weather.map(parseLine)
    minTemps = cleanData.filter(lambda x: "TMIN" in x[1])
    stationTemp = minTemps.map(lambda x : (x[0], x[2]))
    results = stationTemp.reduceByKey(lambda x, y : min(x, y))
    
    for result in results.collect():
        print(result)
    

if __name__ == "__main__":
    main()