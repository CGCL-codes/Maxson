###Maxson
 we  start  with  a  study  with  a  real  production  workload in  Alibaba,  which  consists  of  over  3  million  queries  on  JSON.Our  study  reveals  significant temporal and spatial correlations among those queries, which result in massive redundant parsing operations  among  queries.  Instead  of  repetitively  parsing  theJSON data, we propose to develop a cache system named Maxsonfor  caching  the  JSON  query  results  (the  values  evaluated  fromJSONPath)  for  reuse.  Specifically,  we  develop  effective  machinelearning-based   predictor   with   combining   LSTM   (long   short-term memory) and CRF (conditional random field) to determinethe   JSONPaths   to   cache   given   the   space   budget.   We   have implemented  Maxson  on  top  of  SparkSQL.  

###Structure of Maxson

![Alt text](./1584011436124.png)


Maxson is designed to be fully compatible with SparkSQL,a  Spark  module  for  structured  data  processing.  Users  canexecute  SQL  queries  and  support  reading  and  writing  datastored in Hive. SparkSQL compiles SQL queries into physical plans  to  be  executed  on  a  cluster.  
A  physical  plan  is  a  set of  RDD   operations  that  are  executed  on  the  data  source,typically contains scan,filter,projection,join, etc. To make aphysical plan to access the cache table, we implemented Maxson Parser based on SparkSQL parser. When the Maxson Parser compiles SQL statement into a physical plan, if a JSONPathin  the  SQL  statement  hits  a  valid  cached  value,  it  generates a  placeholder  that  stores  the  JSONPath  information  and  thereference to the cache table. A cache item is valid if the cached time is behind the last modification time of raw data table. If the query needs to access both cached and uncached data, then during the table scan phase, we use Value Combiner to stitch the cached and uncached data into complete records.


###Building and  configuration
Maxson is built using Apache Maven. To build Maxson and its example programs, run:

```build/mvn -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.3 -Phive -Phive-thriftserver -DskipTests package
```
We   have implemented  Maxson  on  top  of  Spark. The configuraion is same to the Spark.

http://spark.apache.org/




