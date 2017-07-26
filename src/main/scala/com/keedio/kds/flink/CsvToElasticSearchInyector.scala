package com.keedio.kds.flink

import java.util

import com.keedio.kds.flink.config.FlinkProperties
import com.keedio.kds.flink.models.Assessment
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSink, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.log4j.Logger
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}

/**
  * Created by luislazaro on 21/7/17.
  * lalazaro@keedio.com
  * Keedio
  */
class CsvToElasticSearchInyector

object CsvToElasticSearchInyector {

  def main(args: Array[String]): Unit = {
    val LOG = Logger.getLogger(classOf[CsvToElasticSearchInyector])
    lazy val flinkProperties = new FlinkProperties(args)
    lazy val properties: flinkProperties.FlinkProperties.type = flinkProperties.FlinkProperties
    val pathTocsv = "./src/test/resources/kdsMexicanFood.csv"
    //use of streaming environment
    val envStreaming: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    envStreaming.setParallelism(1)
    //source
    val dataStreaming: DataStream[String] = envStreaming.readTextFile(pathTocsv)

    //tranformation
    val dataStreamAssesment: DataStream[Assessment] = dataStreaming.map(s => Assessment(s))

    //sink
    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", "KDS_Seman")
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")
    val transportAddresses = new util.ArrayList[TransportAddress]()
    transportAddresses.add(new InetSocketTransportAddress("ambari1.ambari.keedio.org", 9300))
    transportAddresses.add(new InetSocketTransportAddress("ambari2.ambari.keedio.org", 9300))
    transportAddresses.add(new InetSocketTransportAddress("ambari3.ambari.keedio.org", 9300))

    val a: DataStreamSink[Assessment] = dataStreamAssesment
      .addSink(new ElasticsearchSink[Assessment](config, transportAddresses, new ElasticsearchSinkFunction[Assessment] {
        def createIndexRequest(element: Assessment): IndexRequest = {
          val json = new util.HashMap[String, String]
          json.put("comment", element.comment)
          json.put("customerSupport", element.customerSupport)
          json.put("food", element.food)
          json.put("place", element.place)
          json.put("customerSupport", element.customerSupport)
          json.put("price", element.price)
          json.put("sanitation", element.sanitation)
          json.put("otherStaff", element.otherStaff)


          return Requests.indexRequest()
            .index("assessments")
            .`type`("assessment")
            .source(json)
        }

        override def process(t: Assessment, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }))

    //execute streaming environment
    envStreaming.execute()
  }

}