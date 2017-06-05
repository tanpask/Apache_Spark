
#![Spark Logo](http://spark-mooc.github.io/web-assets/images/ta_Spark-logo-small.png) + ![Python Logo](http://spark-mooc.github.io/web-assets/images/python-logo-master-v3-TM-flattened_small.png)
# **Spark Tutorial: Learning Apache Spark**
#### [Apache Spark](http://spark.apache.org/) is a cluster computing platform designed to be fast and general purpose.


#### Every Spark application consists of a driver program that launches paralel operations on a cluster. Driver programs access Spark through a SparkContext object, which represents a connection to a computing cluster.
#### *Spark Context*
#### We can execute python code using this IPython notebook. But, sisnce no Spark functionality is actually being used, no tasks are launched on the executors. In order to use Spark and its API we will need to use a `SparkContext`.  When running Spark, you start a new Spark application by creating a [SparkContext](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.SparkContext).  


```python
# Display the type of the Spark Context sc
type(sc)
```




    pyspark.context.SparkContext



### **Transformations and actions with RDDs**

#### Since RDD is immutable, Users create RDDs in two ways: by loading an external dataset, or by running Transformation on a pre-existing RDD. 
####There are second type of operations called Actions. They compute a result based on an RDD, and either return it to the driver program or save it to an external storage system (e.g., HDFS). 


####**Lets create our first RDD**   
####First, we generate dummy data by creating a list of numbers.


```python
data = xrange(1, 4561,2)


# We can check the size of the list and print out its first element

print len(data)
print data[0]
```

    2280
    1



#### To create the RDD, we use `sc.parallelize()`, which tells Spark to create a new set of input data based on data that is passed in. 



```python
# Parallelize data using 8 partitions
# This operation is a transformation of data into an RDD
# Spark uses lazy evaluation, so no Spark jobs are run at this point
firstRDD = sc.parallelize(data, 4)
```

#### **Transformations**
####One of the most common transformation is Map.  Essentially, it applies a function to each element of the dataset, and outputs resulting dataset of the same length. 
#### Now we will use `map()` to increase per unit each value in the  firstRDD we just created. 
#### It is important to remember, that Spark passes functions. So, we have to create a function or use lambda (unnamed) functions.


```python
# Lets use lambda function. It should be: lambda x: x+1
plusOneRDD=firstRDD.map(lambda x:x+1)
```

#### ** Actions  **
#### To see the resulting list we can usethe function collect().
#### The `collect()` method is the first action operation that we have encountered.  Action operations cause Spark to perform the (lazy) transformation operations that are required to compute the RDD returned by the action.  In our example, this means that tasks will now be launched to perform the `parallelize`, `map`, and `collect` operations.


```python
# Let's collect the data
plusOneRDD.collect()
```




    [2,
     4,
     6,
     8,
     10,
     12,
     14,
     16,
     18,
     20,
     22,
     24,
     26,
     28,
     30,
     32,
     34,
     36,
     38,
     40,
     42,
     44,
     46,
     48,
     50,
     52,
     54,
     56,
     58,
     60,
     62,
     64,
     66,
     68,
     70,
     72,
     74,
     76,
     78,
     80,
     82,
     84,
     86,
     88,
     90,
     92,
     94,
     96,
     98,
     100,
     102,
     104,
     106,
     108,
     110,
     112,
     114,
     116,
     118,
     120,
     122,
     124,
     126,
     128,
     130,
     132,
     134,
     136,
     138,
     140,
     142,
     144,
     146,
     148,
     150,
     152,
     154,
     156,
     158,
     160,
     162,
     164,
     166,
     168,
     170,
     172,
     174,
     176,
     178,
     180,
     182,
     184,
     186,
     188,
     190,
     192,
     194,
     196,
     198,
     200,
     202,
     204,
     206,
     208,
     210,
     212,
     214,
     216,
     218,
     220,
     222,
     224,
     226,
     228,
     230,
     232,
     234,
     236,
     238,
     240,
     242,
     244,
     246,
     248,
     250,
     252,
     254,
     256,
     258,
     260,
     262,
     264,
     266,
     268,
     270,
     272,
     274,
     276,
     278,
     280,
     282,
     284,
     286,
     288,
     290,
     292,
     294,
     296,
     298,
     300,
     302,
     304,
     306,
     308,
     310,
     312,
     314,
     316,
     318,
     320,
     322,
     324,
     326,
     328,
     330,
     332,
     334,
     336,
     338,
     340,
     342,
     344,
     346,
     348,
     350,
     352,
     354,
     356,
     358,
     360,
     362,
     364,
     366,
     368,
     370,
     372,
     374,
     376,
     378,
     380,
     382,
     384,
     386,
     388,
     390,
     392,
     394,
     396,
     398,
     400,
     402,
     404,
     406,
     408,
     410,
     412,
     414,
     416,
     418,
     420,
     422,
     424,
     426,
     428,
     430,
     432,
     434,
     436,
     438,
     440,
     442,
     444,
     446,
     448,
     450,
     452,
     454,
     456,
     458,
     460,
     462,
     464,
     466,
     468,
     470,
     472,
     474,
     476,
     478,
     480,
     482,
     484,
     486,
     488,
     490,
     492,
     494,
     496,
     498,
     500,
     502,
     504,
     506,
     508,
     510,
     512,
     514,
     516,
     518,
     520,
     522,
     524,
     526,
     528,
     530,
     532,
     534,
     536,
     538,
     540,
     542,
     544,
     546,
     548,
     550,
     552,
     554,
     556,
     558,
     560,
     562,
     564,
     566,
     568,
     570,
     572,
     574,
     576,
     578,
     580,
     582,
     584,
     586,
     588,
     590,
     592,
     594,
     596,
     598,
     600,
     602,
     604,
     606,
     608,
     610,
     612,
     614,
     616,
     618,
     620,
     622,
     624,
     626,
     628,
     630,
     632,
     634,
     636,
     638,
     640,
     642,
     644,
     646,
     648,
     650,
     652,
     654,
     656,
     658,
     660,
     662,
     664,
     666,
     668,
     670,
     672,
     674,
     676,
     678,
     680,
     682,
     684,
     686,
     688,
     690,
     692,
     694,
     696,
     698,
     700,
     702,
     704,
     706,
     708,
     710,
     712,
     714,
     716,
     718,
     720,
     722,
     724,
     726,
     728,
     730,
     732,
     734,
     736,
     738,
     740,
     742,
     744,
     746,
     748,
     750,
     752,
     754,
     756,
     758,
     760,
     762,
     764,
     766,
     768,
     770,
     772,
     774,
     776,
     778,
     780,
     782,
     784,
     786,
     788,
     790,
     792,
     794,
     796,
     798,
     800,
     802,
     804,
     806,
     808,
     810,
     812,
     814,
     816,
     818,
     820,
     822,
     824,
     826,
     828,
     830,
     832,
     834,
     836,
     838,
     840,
     842,
     844,
     846,
     848,
     850,
     852,
     854,
     856,
     858,
     860,
     862,
     864,
     866,
     868,
     870,
     872,
     874,
     876,
     878,
     880,
     882,
     884,
     886,
     888,
     890,
     892,
     894,
     896,
     898,
     900,
     902,
     904,
     906,
     908,
     910,
     912,
     914,
     916,
     918,
     920,
     922,
     924,
     926,
     928,
     930,
     932,
     934,
     936,
     938,
     940,
     942,
     944,
     946,
     948,
     950,
     952,
     954,
     956,
     958,
     960,
     962,
     964,
     966,
     968,
     970,
     972,
     974,
     976,
     978,
     980,
     982,
     984,
     986,
     988,
     990,
     992,
     994,
     996,
     998,
     1000,
     1002,
     1004,
     1006,
     1008,
     1010,
     1012,
     1014,
     1016,
     1018,
     1020,
     1022,
     1024,
     1026,
     1028,
     1030,
     1032,
     1034,
     1036,
     1038,
     1040,
     1042,
     1044,
     1046,
     1048,
     1050,
     1052,
     1054,
     1056,
     1058,
     1060,
     1062,
     1064,
     1066,
     1068,
     1070,
     1072,
     1074,
     1076,
     1078,
     1080,
     1082,
     1084,
     1086,
     1088,
     1090,
     1092,
     1094,
     1096,
     1098,
     1100,
     1102,
     1104,
     1106,
     1108,
     1110,
     1112,
     1114,
     1116,
     1118,
     1120,
     1122,
     1124,
     1126,
     1128,
     1130,
     1132,
     1134,
     1136,
     1138,
     1140,
     1142,
     1144,
     1146,
     1148,
     1150,
     1152,
     1154,
     1156,
     1158,
     1160,
     1162,
     1164,
     1166,
     1168,
     1170,
     1172,
     1174,
     1176,
     1178,
     1180,
     1182,
     1184,
     1186,
     1188,
     1190,
     1192,
     1194,
     1196,
     1198,
     1200,
     1202,
     1204,
     1206,
     1208,
     1210,
     1212,
     1214,
     1216,
     1218,
     1220,
     1222,
     1224,
     1226,
     1228,
     1230,
     1232,
     1234,
     1236,
     1238,
     1240,
     1242,
     1244,
     1246,
     1248,
     1250,
     1252,
     1254,
     1256,
     1258,
     1260,
     1262,
     1264,
     1266,
     1268,
     1270,
     1272,
     1274,
     1276,
     1278,
     1280,
     1282,
     1284,
     1286,
     1288,
     1290,
     1292,
     1294,
     1296,
     1298,
     1300,
     1302,
     1304,
     1306,
     1308,
     1310,
     1312,
     1314,
     1316,
     1318,
     1320,
     1322,
     1324,
     1326,
     1328,
     1330,
     1332,
     1334,
     1336,
     1338,
     1340,
     1342,
     1344,
     1346,
     1348,
     1350,
     1352,
     1354,
     1356,
     1358,
     1360,
     1362,
     1364,
     1366,
     1368,
     1370,
     1372,
     1374,
     1376,
     1378,
     1380,
     1382,
     1384,
     1386,
     1388,
     1390,
     1392,
     1394,
     1396,
     1398,
     1400,
     1402,
     1404,
     1406,
     1408,
     1410,
     1412,
     1414,
     1416,
     1418,
     1420,
     1422,
     1424,
     1426,
     1428,
     1430,
     1432,
     1434,
     1436,
     1438,
     1440,
     1442,
     1444,
     1446,
     1448,
     1450,
     1452,
     1454,
     1456,
     1458,
     1460,
     1462,
     1464,
     1466,
     1468,
     1470,
     1472,
     1474,
     1476,
     1478,
     1480,
     1482,
     1484,
     1486,
     1488,
     1490,
     1492,
     1494,
     1496,
     1498,
     1500,
     1502,
     1504,
     1506,
     1508,
     1510,
     1512,
     1514,
     1516,
     1518,
     1520,
     1522,
     1524,
     1526,
     1528,
     1530,
     1532,
     1534,
     1536,
     1538,
     1540,
     1542,
     1544,
     1546,
     1548,
     1550,
     1552,
     1554,
     1556,
     1558,
     1560,
     1562,
     1564,
     1566,
     1568,
     1570,
     1572,
     1574,
     1576,
     1578,
     1580,
     1582,
     1584,
     1586,
     1588,
     1590,
     1592,
     1594,
     1596,
     1598,
     1600,
     1602,
     1604,
     1606,
     1608,
     1610,
     1612,
     1614,
     1616,
     1618,
     1620,
     1622,
     1624,
     1626,
     1628,
     1630,
     1632,
     1634,
     1636,
     1638,
     1640,
     1642,
     1644,
     1646,
     1648,
     1650,
     1652,
     1654,
     1656,
     1658,
     1660,
     1662,
     1664,
     1666,
     1668,
     1670,
     1672,
     1674,
     1676,
     1678,
     1680,
     1682,
     1684,
     1686,
     1688,
     1690,
     1692,
     1694,
     1696,
     1698,
     1700,
     1702,
     1704,
     1706,
     1708,
     1710,
     1712,
     1714,
     1716,
     1718,
     1720,
     1722,
     1724,
     1726,
     1728,
     1730,
     1732,
     1734,
     1736,
     1738,
     1740,
     1742,
     1744,
     1746,
     1748,
     1750,
     1752,
     1754,
     1756,
     1758,
     1760,
     1762,
     1764,
     1766,
     1768,
     1770,
     1772,
     1774,
     1776,
     1778,
     1780,
     1782,
     1784,
     1786,
     1788,
     1790,
     1792,
     1794,
     1796,
     1798,
     1800,
     1802,
     1804,
     1806,
     1808,
     1810,
     1812,
     1814,
     1816,
     1818,
     1820,
     1822,
     1824,
     1826,
     1828,
     1830,
     1832,
     1834,
     1836,
     1838,
     1840,
     1842,
     1844,
     1846,
     1848,
     1850,
     1852,
     1854,
     1856,
     1858,
     1860,
     1862,
     1864,
     1866,
     1868,
     1870,
     1872,
     1874,
     1876,
     1878,
     1880,
     1882,
     1884,
     1886,
     1888,
     1890,
     1892,
     1894,
     1896,
     1898,
     1900,
     1902,
     1904,
     1906,
     1908,
     1910,
     1912,
     1914,
     1916,
     1918,
     1920,
     1922,
     1924,
     1926,
     1928,
     1930,
     1932,
     1934,
     1936,
     1938,
     1940,
     1942,
     1944,
     1946,
     1948,
     1950,
     1952,
     1954,
     1956,
     1958,
     1960,
     1962,
     1964,
     1966,
     1968,
     1970,
     1972,
     1974,
     1976,
     1978,
     1980,
     1982,
     1984,
     1986,
     1988,
     1990,
     1992,
     1994,
     1996,
     1998,
     2000,
     ...]




```python
plusOneRDD.takeSample(True,10)
```




    [2930, 1526, 3492, 2654, 1628, 4300, 2794, 1432, 4228, 1426]



#### Another usefull action is count(). It counts the number of elements in an RDD. 



```python
print plusOneRDD.count()
```

    2280


### EXERCISE 1


```python
# TODO

# by using map() and lambda function add 5 to each element of firstRDD
plusfiveRDD = firstRDD.<FILL IN>

print plusfiveRDD.take(10)
```


```python
# TEST
import sys
sys.path.append("/usr/local/lib/python2.7/site-packages")

import hashlib
hashed = lambda x: hashlib.sha1(str(x)).hexdigest()

from test_helper import Test

f = plusfiveRDD.first()


Test.assertEqualsHashed(f, 'c1dfd96eea8cc2b62785275bca38ac261256e278', 'Test 1 success')
Test.assertEqualsHashed(plusfiveRDD.take(10)[-1], '4d134bc072212ace2df385dae143139da74ec0ef', 'Test 2 success')
```

#### Now, let's filter our plusOneRDD. 
#### We'll create a new RDD that only contains the values less than 100 by using the `filter(f)` data-parallel operation. This method is a transformation operation that creates a new RDD from the input RDD by applying filter function `f` to each item in the parent RDD and only passing those elements where the filter function returns `True`. Elements that do not return `True` will be dropped. 


```python
# Define a function to filter a single value
def flt(value):
    if (value < 100):
        return True
    else:
        return False
filteredRDD = plusOneRDD.filter(flt)

# Since filter is a transformation, we have to use action to preform calculations 
# and get resulting data
print filteredRDD.collect()
```

    [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 98]


### EXERCISE 2

#### Now, we allow you filter your plusfiveRDD with numbers divided by 3.  


```python
# TODO

# define the function that check or value is divide by 3 or not
def devided_by_3_number(value):
    <FILL IN>
    return <FILL IN>

# filter your RDD
filteredNewRDD = plusfiveRDD.<FILL IN>

# print the list of number
print <FILL IN>
```


```python
# TEST 
from test_helper import Test

f_10 = filteredNewRDD.take(10)
t_1 = filteredNewRDD.top(1)[0]


Test.assertEqualsHashed(f_10, '5199c0bbcd0c8779c4cf6cb9cec2b9619ce161ca', 'Test 3 success')
Test.assertEqualsHashed(t_1, 'f2454d82ad9bd30810f80d8f76ce3ea3e50c9ce5', 'Test 4 success')
```

#### ** Data displaying**

#### There are few other frequently used actions:[first()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.first), [take()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.take), [top()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.top), [takeOrdered()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.takeOrdered), and [reduce().](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.reduce)
#### In order to get rough understanding of the data through visual inspection    `first()`, `take()`, `top()`, and `takeOrdered()` actions are used. Note that for the `first()` and `take()` actions, the elements that are returned depend on how the RDD is *partitioned*.
#### The `take(n)` action to returns the first n elements of the RDD. 
####The `first()` action returns the first element of an RDD, and is equivalent to `take(1)`.
#### The `takeOrdered(n)` action returns the first n elements of the RDD, using either their natural order or a custom comparator. 
####The `top(n)` action is similar to `takeOrdered(n)` except that it returns the list in *descending order.*
#### The `reduce()` action reduces the elements of a RDD to a single value by applying a function that takes two parameters and returns a single value.  The function should be commutative and associative, as `reduce()` is applied at the partition level and then again to aggregate results from partitions.  If these rules don't hold, the results from `reduce()` will be inconsistent.  Reducing locally at partitions makes `reduce()` very efficient.


```python
# Let's get the first element
print filteredRDD.first()
```


```python
# The first 4
print filteredRDD.take(10)
```


```python
# Retrieve the three smallest elements
print filteredRDD.takeOrdered(3)
```


```python
# Retrieve the five largest elements
print filteredRDD.top(2)
```


```python
# Pass a lambda function to takeOrdered to reverse the order
filteredRDD.takeOrdered(4, lambda s: -s)
```


```python
# Getting Python's add function
from operator import add
# Sum the RDD using reduce
print filteredRDD.reduce(add)
# Sum using reduce with a lambda function
print filteredRDD.reduce(lambda a, b: a + b)
```

### EXERCISE 3


```python
# TODO

# filter your filteredNewRDD with number which are less than 50
filteredNewRDD_part = filteredNewRDD.<FILL IN>

# Print top 5 the greatest numbers in your filteredNewRDD_part
filteredNewRDD_5 = filteredNewRDD_part.<FILL IN>


# Multiply all numbers from  filteredNewRDD_part and print result
Multi_filteredRDD = filteredNewRDD_part.<FILL IN>


print Multi_filteredRDD
```


```python
# TEST 

Test.assertEqualsHashed(filteredNewRDD_5, '8ff5b2b55bc143e048bf78182ec3194f1e49e0c7', 'Test 5 success')
Test.assertEqualsHashed(Multi_filteredRDD, 'cb78c76bd7a7cfe8feb17ad7a4e1166dcfbbb7d3', 'Test 6 success')
```

#### The `countByValue()` action returns the count of each unique value in the RDD as a dictionary that maps values to counts.


```python
newRDD = sc.parallelize(["d", "s", "d", "d", "r", "ee", "rr", "r", "r"])
print newRDD.countByValue()
```

### EXERCISE 4


```python
import re
# Define the text variable "I have a cat. The cat is very nice. I love my cat very much."

text = <FILL IN>


# Create RDD with all letters and symbols from the text
# Before remove all spaces from text and move all latters to lowercase
# here you can use the library re (read about it here https://docs.python.org/2/library/re.html) or the function 'replace()'

textRDD = <FILL IN>

# Count the number of each symbol 

n = textRDD.<FILL IN>
```


```python
# TEST 

Test.assertEqualsHashed(len(n), '1574bddb75c78a6fd2251d61e2993b5146201319', 'Test 7 success')
Test.assertEqualsHashed(textRDD.take(20)[10], '8efd86fb78a56a5145ed7739dcb00c78581c5375', 'Test 8 success')
```

### ** More RDD transformations **

####   [flatMap()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.flatMap) transformation is similar to `map()`, except that with `flatMap()` each input item can be mapped to zero or more output elements.

### EXERCISE 5


```python
# Creating new RDD
data = ['foo', 'bar', '1', '2']
dataRDD = sc.parallelize(data, 4)

# From each peice of data create pair that consists from original data and original data + letter 's'. 
# Use map
pairsRDD = dataRDD.<FILL IN>

# In this section you should create the pairs ('foo', 'foods') and than use flatMap to create a rdd like ['foo', 'foos', ...]
pairsRDDfmap = dataRDD.<FILL IN>

# View the results with collecting the data
print pairsRDD.<FILL IN>
print pairsRDDfmap.<FILL IN>
```


```python
# TEST 
Test.assertEqualsHashed(pairsRDD.count(), '1b6453892473a467d07372d45eb05abc2031647a', 'Test 9 success')
Test.assertEqualsHashed(pairsRDDfmap.count(), 'fe5dbbcea5ce7e2988b8c69bcfdfde8904aabc1f', 'Test 10 success')
```

#### The approach using map transformation is often used to create key-value pairs.


#### Let's investigate transformations that are used with key-value pairs: [groupByKey()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.groupByKey) and [reduceByKey()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.reduceByKey).

#### The `reduceByKey()` transformation gathers together pairs that have the same key and applies a function to two associated values at a time. `reduceByKey()` operates by applying the function first within each partition on a per-key basis and then across the partitions.
#### While both the `groupByKey()` and `reduceByKey()` transformations can often be used to solve the same problem and will produce the same answer, the `reduceByKey()` transformation works much better for large distributed datasets. 

#### Here are more transformations to prefer over `groupByKey()`:
  + #### [combineByKey()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.combineByKey) 
####can be used when you are combining elements but your return type differs from your input value type.
  + #### [foldByKey()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.foldByKey) 
#### merges the values for each key using an associative function and a neutral "zero value".
#### Now let's go through a simple `groupByKey()` and `reduceByKey()` example.


```python
values=["apple", "banana", "apple", "papaya", "mango", "prune", "mango"]
valuesRDD = sc.parallelize(values,4)
keyvalueRDD=valuesRDD.map(lambda x: (x,1))

# Different ways to sum by key
print keyvalueRDD.groupByKey().map(lambda (k, v): (k, sum(v))).collect()
# Using mapValues, which is recommended when they key doesn't change
print keyvalueRDD.groupByKey().mapValues(lambda x: sum(x)).collect()
# reduceByKey is more efficient / scalable
print keyvalueRDD.reduceByKey(add).collect()
```

#### If we have need to get a list of distinct elements we can use 'distinct()' transformation:


```python
print valuesRDD.collect()
print valuesRDD.distinct().collect()
```

### EXERCISE 6


```python
number_list = [1, 3, 4, 1, 4, 7, 12, 3, 4, 2, 2, 6, 2, 1, 9]

# Create RDD with numbers
valuesRDD_num = <FILL IN>

# Create key\value pair of RDD with each value of number_list and 1
keyvalueRDD_num = valuesRDD_num.<FILL IN>

# Use groupByKey()  or reduceByKey()  to count the number of duplicates of each element
# Print list from key value RDD for numbers 1, 2, 3

keyvalueRDD_num_ordered = keyvalueRDD_num.<FILL IN>
keyvalueRDD_num_ordered
```


```python
# TEST 

Test.assertEqualsHashed(keyvalueRDD_num.take(3)[0], 'd17c7c5e7c759de38db5cb6577a9a60e8ef9456a', 'Test 11 success')
Test.assertEqualsHashed(keyvalueRDD_num_ordered, 'd0a4b212ce0266e5c798105a7312ef88425b6c62', 'Test 12 success')
```

#### ** Caching RDDs **
#### If you need to use the same RDD more than once it can be usefull to  cache it using function cache(). However, if you cache too many RDDs and Spark runs out of memory, it will delete the least recently used (LRU) RDD first. Again, the RDD will be automatically recreated when accessed.


```python
# Name the RDD
filteredRDD.setName('My Filtered RDD')
# Cache the RDD
filteredRDD.cache()
# Is it cached
print filteredRDD.is_cached
```

#### ** Let's have some practice: Simple inventory management system **
#### 
RDD1- 1 dep
RDD2- 2 dep
RDDall

Diff 1/2
Add objects
Catalog all



create 2 RDD ,    compare join key-value pairs 


```python
# Theese are the lists of inventories of the 2 warehouses
inventory1=['Hammer', 'nail', 'Nail', 'screwdriver', 'Backpack', 'Bolt D9', 'Nut D9','Bolt D9', 'Nut D9','Bolt D9', 'nut D12','Bolt D12', 'nut D9','Bolt D9', 'Nut D12']
inventory2=['Bolt D8', 'nut D8','Screwdriver', 'Backpack', 'Bolt D9','screwdriver', 'backpack', 'Bolt D9', 'First Aid Kit']
```

####Create an RDD for each warehouse. Be aware that some goods are written with uppercase letters. You have to use lower() function from Python.


```python
RDD1=sc.parallelize(inventory1, 4).map(lambda x: x.lower())
RDD2=sc.parallelize(inventory2, 4).map(lambda x: x.lower())
print RDD1.collect()
print RDD2.collect()
```

#### Combine both RDDs.
####You can use [union()](http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=union#pyspark.RDD.union)


```python
RDDall=RDD1.union(RDD2)
```

####Calculate ammounts of goods by using map() and reduceByKey(). 


```python
pairRDDall=RDDall.map(lambda x:(x,1)).reduceByKey(add)
print pairRDDall.collect()
```

### EXERCISE 7

####Find out what goods are stored in warehouse 1 but not in warehouse 2. (you can use  [subtract(other, numPartitions=None)](http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=subtract#pyspark.RDD.subtract))


```python
# Subtract RDD1 with RDD2
# Create RDD pairs (goods, number of goods)
# you can use .map and .reduceByKey

diffRDD=RDD1.<FILL IN> # Use et least 2 functions

diff = diffRDD.collect()
```

#### Sort diffRDD alphabeticaly (using [sortByKey() fucntion])(http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=sortbykey#pyspark.RDD.sortByKey) and print its result.


```python
diff_sorted = diffRDD.<FILL IN>
```


```python
# TEST 

Test.assertEqualsHashed(diffRDD.take(1), '280153e0c50e105f8d2380fc64fe4a2f162abfe9', 'Test 13 success')
Test.assertEqualsHashed(diff_sorted[1], '5c9da466e4f3799ea3a9aa7950ef06b23db3fda4', 'Test 14 success')
Test.assertEqualsHashed(diff_sorted, '89066489ead47cae2fa3737e8c4a82bc3e9afbd6', 'Test 15 success')
```

THE END

<center><h3>Presented by <a target="_blank" rel="noopener noreferrer nofollow" href="http://datascience-school.com">datascience-school.com</a></h3></center>
