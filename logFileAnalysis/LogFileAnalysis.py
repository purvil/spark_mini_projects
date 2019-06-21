from pyspark import SparkContext, SparkConf

def isNotHeader(line):
    return not (line.startswith("host") and "bytes" in line)

def createSample():
    conf = SparkConf().setAppName("log").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    julyLog = sc.textFile("data/nasa_19950701.tsv")
    augLog = sc.textFile("data/nasa_19950801.tsv")
    
    combinedLog = julyLog.union(augLog)
    
    # Remove the header line
    cleanLog = combinedLog.filter(isNotHeader)
    
    sample = cleanLog.sample(withReplacement = True, fraction = 0.1)
    sample.saveAsTextFile("out/sample_nasa_logs.csv")
    
def hostAccessedInBothMonth():
    
    conf = SparkConf().setAppName("log").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    julyLog = sc.textFile("data/nasa_19950701.tsv")
    augLog = sc.textFile("data/nasa_19950801.tsv")
    
    julyHost = julyLog.map(lambda line:line.split("\t")[0])
    augHost = augLog.map(lambda line:line.split("\t")[0])
    
    interSection = julyHost.intersection(augHost)
    cleanHosts = interSection.filter(lambda host:host != "host")
    cleanHosts.saveAsTextFile("out/nasa_logs_both_months.csv")
    
if __name__ == "__main__":
    #createSample()
    hostAccessedInBothMonth()