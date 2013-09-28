HBase bulk loading
===============
Java implementation of full bulk load circle. 
Build for Hadoop Cloudera CDH4

The bulk load feature uses a MapReduce job to output table data in HBase's internal data format, and then directly loads
the generated StoreFiles into a running cluster. Using bulk load will use less CPU and network resources than simply
using the HBase API.


A.Nikulin 2anikulin@gmail.com
http://habrahabr.ru/post/195040/
