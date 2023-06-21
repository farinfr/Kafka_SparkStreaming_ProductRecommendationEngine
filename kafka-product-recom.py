import sys
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import SQLContext
from math import sqrt
from kafka import KafkaProducer

global product_filter
global number_of_items , number_of_intersection


sc = SparkContext(appName="PythonStreamingKafkaProduct")
sc.setLogLevel("ERROR") # Removing INFO logs.
ssc = StreamingContext(sc, 1)
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def makePairs(element):
    (user , (item1 , item2)) = element
    return ((str(item1), str(item2)), 1)

def filterDuplicates(element):
    (user , (item1 , item2)) = element
    return item1 < item2

def sortPair(item):
    if item[0] > item[1]:
        return (item[1] , item[0])
    return (item[0] , item[1])

def reform(x):
    ((user , time) , pair) = x
    return (user , pair)

data = sc.textFile("data/user-item.csv")
#create number of intersecton of two item rdd
ratings = data.map(lambda l: l.split(",")).map(lambda l: ((int(l[0]) ,str(l[3]) ) , int(l[1]) ))
joinedRatings = ratings.join(ratings).map(reform)
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
itemPairs = uniqueJoinedRatings.map(makePairs)
number_of_intersection = itemPairs.reduceByKey( lambda a, b: a + b )
#create number of items dictionary
items = data.map(lambda l: l.split(",")).map(lambda x : x[1]).map(lambda x : ( x , 1 ))
number_of_items = items.reduceByKey( lambda a, b: a + b )
number_of_items_set = {}
for element in number_of_items.collect():
    (item , num) = element
    number_of_items_set[item] = num
#create user item dictionary
users = data.map(lambda l: l.split(",")).map(lambda x: x[0] )
users = {i for i in users.collect()}
user_item_dic = {}
for user in users:
    user_item_dic[user] = {}
user_item = data.map(lambda l: l.split(",")).map(lambda x: (x[0] , x[1]) ).collect()
for ui in user_item:
    (user , item) = ui
    user_item_dic[user][item] = 0
    
for ui in user_item:
    (user , item) = ui
    user_item_dic[user][item] +=1

#create name of item dictionary
def loadItemNames():
        itemNames = {}
        with open("data/item-data.csv") as f:
            for line in f:
                fields = line.split(",")
                itemNames[int(fields[0])] = fields[1]
        return itemNames
nameDict = loadItemNames()


def recomandiation ( record ):
    global product_filter 
    
    # Extract similarities for the item we care about that are "good".
    scoreThreshold = 0.7
    coOccurenceThreshold = 5

    # responseObj = {}
    try:
        productId = int(record) # Here we get the productId
        product_filter = record
    except:
        print('productId is incorrect')
        return
        
    # findding pairs that are candidate to similarity
    def find_pairs(pair):
        (items , num) = pair
        (item1 , item2) = items
        return item1 == product_filter or item2 == product_filter
    def filter_num (pair):
        (items , num) = pair
        return num > coOccurenceThreshold

    filtered_pair = number_of_intersection.filter(find_pairs)
    filtered_pair = filtered_pair.filter(filter_num)
    

    def add_similarity(pair):
        global product_filter
        (items , num) = pair
        (item1 , item2) = items
        number_item1  = number_of_items_set[item1]
        number_item2  = number_of_items_set[item2]
        # ( c , number_item2 ) = number_of_items.filter(lambda x: filter_item(x)).collect()[0]
        similarity = num/((number_item1**(1/2))*(number_item2**(1/2)))
        if(similarity > scoreThreshold):
            return (item1 , item2 ,number_item1,number_item2,num ,similarity)

    results = [add_similarity(i)for i in filtered_pair.collect()]

    #there is no similar item
    if len(results) == 0 :
        # print('no item found')
        return
    
    #printing result
    try:
        print("Top similar products for: " + nameDict[productId])
    except:
        print("Top similar products for: " + str(productId))
    print('----------------------------')
    
    for result in results:
        if result == None:
            continue
        print(result);
        (item1 , item2 ,number_item1,number_item2,num, sim) = result
        similarProductId = int(item1)
        if similarProductId == productId:
            similarProductId = int(item2)
        print(similarProductId)
        if similarProductId in nameDict:
            print(nameDict[similarProductId] + " | " + str(similarProductId))
            # responseObj[similarProductId] = nameDict[similarProductId]
        else:
            print('Name of product not found!'+ " | " + str(similarProductId))
        print('**************')
    print('----------------------------')
    # else:
    #     print("productId is incorrect")
        # producer.send('prodRecommSend', repr(bytearray(responseObj)))
        # producer.flush()
    return

def add_sell(records):
    global number_of_items , number_of_intersection
    
    print("\nLoading product names...")
    user = records.pop(0)
    if user not in users :
        user_item_dic[user] = {}
        users.add(user)
    #update user item dictionary
    for item in records:
        try:
            user_item_dic[user][item] +=1
        except:
            user_item_dic[user][item] = 1
    print(user_item_dic[user])
    #update number of itmes set
    for i in range(0 , len(records)):
        if records[i] not in number_of_items_set:
            number_of_items_set[records[i]] = 1
        else:
            number_of_items_set[records[i]] = number_of_items_set[records[i]] + 1
    #update number of intersection
    new_pairs = []
    for i in range(0,len(records)-1):
        for j in range(i+1,len(records)):
            new_pairs.append((records[i] , records[j]))        
    new_pairs = sc.parallelize(new_pairs).map(sortPair)    
    new_pairs = new_pairs.map(lambda x:( x , 1 ))
    number_of_intersection = number_of_intersection.union(new_pairs)
    number_of_intersection = number_of_intersection.reduceByKey( lambda a, b: a + b )

def recomandiation_user (record):
    user = record.pop(0)
    if user not in users:
        print("user not found!")
        return
    
    print('recomandiation for user ' + str(user))
    freq_item = list(user_item_dic[user].keys())[0:10]
    for item in freq_item:
        recomandiation(item)


def runAlgorithm(message):
    global product_filter
    global number_of_items , number_of_intersection
    records = message.collect()
    
    if len(records) == 0:
        return
    flag = records[0].pop(0)
    if flag == '1':
        for record in records[0]:
            recomandiation(record)
    elif flag == '2':
        add_sell(records[0])
    elif flag == '3':
        recomandiation_user(records[0])



    
    
def checkQuery ( x ):
    msg = x[1]
    msg = msg.split(' ')
    return msg       
def main():
    if len(sys.argv) != 3:
        print("Usage: kafka-product-recom.py <zk> <topic>")
        exit(-1)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    # lines = kvs.map(lambda x: x[1])
    lines = kvs.map(checkQuery)
    lines.pprint() # To print the status and time.
    lines.foreachRDD(runAlgorithm)

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()

