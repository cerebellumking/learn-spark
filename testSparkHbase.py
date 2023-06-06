import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F 
import time,os,random
import pandas as pd
import json
import happybase



def main():
    spark = SparkSession.builder \
        .appName("structured streaming") \
        .config("spark.sql.shuffle.partitions","8") \
        .config("spark.default.parallelism","8") \
        .config("master","local[*]") \
        .config("spark.driver.memory", '4G')\
        .config("spark.executor.memory", '4G')\
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    
    # # 创建表
    # conn = happybase.Connection("100.81.9.75")
    # table_name = 'mytable'
    # families = {
    #     'cf': dict(max_versions=1)
    # }
    # # 创建表
    # conn.create_table(table_name, families)
    # 获取表
    pool = happybase.ConnectionPool(size=3, host="100.81.9.75")
    # 获取连接
    with pool.connection() as connection:
        table = connection.table("t_news")
        # 查询数据
        # 方法1：
        # one_row = table.row('row1')  # 获取row1行数据
        # for value in one_row.keys():  # 遍历当前行的每一列
        #     print(value.decode('utf-8'), one_row[value].decode('utf-8'))  # 可能有中文，使用encode转码
    
        # 方法2：
        # 生成0-113762的列表
        # for i in range(113762):
        #     row_key = str(i)
        #     row = table.row(row_key)
        #     if row:
        #         continue
        #     else:
        #         print(row_key, "None")
        
        # for row_index, col_families in table.scan():  # row_key是行index, col_families是列族
        #     # 去掉row_index中的b''
        #     row_index = row_index.decode('utf-8')
        #     # 寻找0-113762中缺少的两个数字
            


            # for col_key, col_value in col_families.items():
            #     col_key_str = col_key.decode('utf-8')
            #     col_value_str = col_value.decode('utf-8')
            #     print("行:{} 列:{} 值:{}".format(row_index, col_key_str, col_value_str))
            #     print("=================")




    # def bulk_insert(batch):
    #     """批量插入，每个 partition 新建一个链接，避免没插入一条就要连接一次"""
    #     table = happybase.Connection(server, port=9090).table(table_name)
    #     for r in batch:
    #         tokens = r.split(",")
    #         key = tokens[0] + "-" + tokens[7]
    #         value = {"info:date": tokens[0]
    #             , "info:open": tokens[1]
    #             , "info:high": tokens[2]
    #             , "info:low": tokens[3]
    #             , "info:close": tokens[4]
    #             , "info:volume": tokens[5]
    #             , "info:adjclose": tokens[6]
    #             , "info:symbol": tokens[0]
    #                  }
    #         # Look at jupyter console to see the print output
    #         print(key, value)
    #         table.put(key, value)

    # stocks.foreachPartition(bulk_insert)
    sc.stop()



if __name__ == '__main__':
    main()




# def read_for_hbase(spark_context):
#     host = '100.81.9.75'
#     table = 'student'
#     conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
#     keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
#     valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
#     hbase_rdd = spark_context.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
#                                    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
#                                    "org.apache.hadoop.hbase.client.Result", keyConverter=keyConv, valueConverter=valueConv,
#                                    conf=conf)
#     count = hbase_rdd.count()
#     print(count)
#     hbase_rdd.cache()
#     output = hbase_rdd.collect()
#     for (k, v) in output:
#         print(k, v)


# def main():
#     spark = SparkSession.builder \
#         .appName("structured streaming") \
#         .config("spark.sql.shuffle.partitions","8") \
#         .config("spark.default.parallelism","8") \
#         .config("master","local[*]") \
#         .config("spark.driver.memory", '4G')\
#         .config("spark.executor.memory", '4G')\
#         .enableHiveSupport() \
#         .getOrCreate()

#     sc = spark.sparkContext

#     read_for_hbase(sc)
#     # write_to_hbase(sc)
#     sc.stop()

    
# if __name__ == '__main__':
#     main()
