from pyspark import SparkConf, SparkContext

def parseRecord(line):
    fields = line.split(',')
    return fields[0], float(fields[2])
    
def main():
    conf = SparkConf().setMaster("local").setAppName("totalMoneySpent")
    sc = SparkContext(conf = conf)
    orders = sc.textFile("data/customer-orders.csv")
    
    customerMoneySpent = orders.map(parseRecord).reduceByKey(lambda x, y : x + y)
    customerMoneySpentSorted = customerMoneySpent.map(lambda x : (x[1], x[0])).sortByKey().map(lambda x : (x[1], x[0]))
    customerMoneySpentSorted.saveAsTextFile("out/amountbyCustomer.txt")
    
    
if __name__ == "__main__":
    main()