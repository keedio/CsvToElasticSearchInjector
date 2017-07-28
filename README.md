## Description
Keedio's CsvToElasticSearchInjector is a simple Flink job to 
inject data into elasticsearch from a common source csv of regular files.

## Main goal
Read csv file via streaming api, parse as pojo 'Assesment' and inject into Elasticsearch.
 
## Available properties
|   property	|   default value	|   mandatory	|   Description and example	|   connector	|
|:-:	|---	|---	|---	|---	|
|input.csv 	|   |  yes 	|  path to input csv file, hdfs:///input/file.csv	| flink api streaming  	|
|field.separator 	|   ,	|no 	| open CsvReader parameter 	|   	|
|cluster.name	|   	|yes   	| elasticsearch's cluster name   "kds_cluster"  	||
|index.name |   	|yes|elasticsearch's index  "books"||
|type.name|   	|yes|elasticsearch's type "book"||
|inet.addresses.rpc|   	|yes | elasticsearch's nodes list:  "ambari1.ambari.keedio.org, 9300; ambari2.ambari.keedio.org, 9300"| flink-connector-elasticsearch_2.10, transport client|


##Yarn
`./bin/flink run -m yarn-cluster -yn 1 -yjm 1024 -ytm 1024
  -c com.keedio.kds.flink.injector.CsvToElastic examples/CsvToElasticSearchInjector-0.0.2-SNAPSHOT.jar
  --properties.file flinkJob_injector.properties`