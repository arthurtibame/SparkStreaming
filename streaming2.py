from __future__ import print_function

import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    
    conf = SparkConf().setAppName("Spark Streaming testing")\
                  .setMaster("spark://10.120.26.200:7077")\
                  .set("spark.executor.memory", '6g')\
                  .set('spark.cores.max', '4')

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN") # 减少shell打印日志
    ssc = StreamingContext(sc, 5) # 5秒的计算窗口
    DStream = ssc.socketTextStream("10.120.26.247",port=9998)    



    lines_rdd = DStream
    # 将workcount结果打印到当前shell    
    lines_rdd.pprint()
   # type(lines_rdd).pprint()
    ssc.start()
    ssc.awaitTermination()
