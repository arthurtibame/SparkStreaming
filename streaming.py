from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    
    conf = SparkConf().setAppName("202")\
                  .setMaster("spark://10.120.26.200:7077")\
                  .set("spark.executor.memory", '6g')\
                  .set('spark.cores.max', '4')

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN") # 减少shell打印日志
    ssc = StreamingContext(sc, 5) # 5秒的计算窗口
    brokers='10.120.26.247:9093'
    topic = 'testing'
    # 使用streaming使用直连模式消费kafka 
    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines_rdd = kafka_streaming_rdd.map(lambda x: x[1])
    counts = lines_rdd.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    # 将workcount结果打印到当前shell    
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
