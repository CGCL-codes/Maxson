## Maxson
 We  start  with  a  study  with  a  real  production  workload in  Alibaba,  which  consists  of  over  3  million  queries  on  JSON. Our study reveals significant temporal and spatial correlations among those queries, which result in massive redundant parsing operations  among  queries.  Instead  of  repetitively  parsing  theJSON data, we propose to develop a cache system named Maxsonfor  caching  the  JSON  query  results  (the  values  evaluated  fromJSONPath)  for  reuse.  Specifically,  we  develop  effective  machinelearning-based   predictor   with   combining   LSTM   (long   short-term memory) and CRF (conditional random field) to determinethe   JSONPaths   to   cache   given   the   space   budget.   We   have implemented  Maxson  on  top  of  SparkSQL.  

## Structure of Maxson

![Structure of Maxson](https://github.com/hzyfox/picture-share/blob/master/mison/architecture.png)

Maxson is designed to be fully compatible with SparkSQL, a  Spark  module  for  structured  data  processing.  Users  canexecute  SQL  queries  and  support  reading  and  writing  datastored in Hive. SparkSQL compiles SQL queries into physical plans  to  be  executed  on  a  cluster.  
A  physical  plan  is  a  set of  RDD   operations  that  are  executed  on  the  data  source, typically contains scan, filter, projection, join, etc. To make aphysical plan to access the cache table, we implemented Maxson Parser based on SparkSQL parser. When the Maxson Parser compiles SQL statement into a physical plan, if a JSONPathin  the  SQL  statement  hits  a  valid  cached  value,  it  generates a  placeholder  that  stores  the  JSONPath  information  and  thereference to the cache table. A cache item is valid if the cached time is behind the last modification time of raw data table. If the query needs to access both cached and uncached data, then during the table scan phase, we use Value Combiner to stitch the cached and uncached data into complete records.

## Other components

- [Maxson training and prediction components can be found here](https://github.com/five-5/Maxson-ML)
- [Some using examples can be found here](https://github.com/hzyfox/spark-json-optimize-examples/)




## Using Maxson

To generate the cache table, you need to configure

```spark.sql.json.writeCache=rue```

When use Maxson to run Example,you need to configure 

``` spark.sql.json.optimize=true```

## Reference Paper

> Hong Huang, Xuanhua Shi, Yipeng Zhang, Zhenyu Hu, Hai Jin, Huan Shen, Yongluan Zhou, Bingsheng He, Ruibo Li, Keyong Zhou. Maxson: Reduce duplicate Parsing Overhead on Raw Data. ICDE 2020: IEEE International Conference on Data Engineering 2020 (Industry and Application track, acceptance rate: 26%).

## Support or Contact
If you have any questions, please contact Yipeng Zhang (<jinyu_zyp@hust.edu.cn>)
