from pyspark import SparkConf, SparkContext

def parseLines(line):
    values = line.split(',')
    stationID = values[0]
    tempType = values[2]
    temperature = float(values[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationID, tempType, temperature)

def main():
    conf = SparkConf().setMaster("local").setAppName("MaxTemperatureByStation")
    sc = SparkContext(conf = conf)
    
    weather = sc.textFile("data/1800.csv")
    cleanWeather = weather.map(parseLines)
    maxTemp = cleanWeather.filter(lambda x:"TMAX" in x[1])
    tempByStation = maxTemp.map(lambda x:(x[0], x[2]))
    maxByStation = tempByStation.reduceByKey(lambda x, y: max(x, y))
 
    for result in maxByStation.collect():
        print(result)


if __name__ == "__main__":
    main()