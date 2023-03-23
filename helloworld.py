import findspark
import pyspark 
from pyspark import SparkContext, SparkConf
#指定spark_home为刚才的解压路径,指定python路径
spark_home = "/home/parallels/Desktop/work/spark-3.3.2-bin-hadoop3"
python_path = "/home/parallels/anaconda3/bin/python3"
findspark.init(spark_home,python_path)

conf = SparkConf().setAppName("test").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("spark version:",pyspark.__version__)
rdd = sc.parallelize(["hello","spark"])
print(rdd.reduce(lambda x,y:x+' '+y))