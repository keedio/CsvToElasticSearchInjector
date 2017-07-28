package com.keedio.kds.flink.injector

import com.keedio.kds.flink.injector.config.FlinkProperties
import com.keedio.kds.flink.injector.models.Assessment
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch._
import org.apache.log4j.Logger
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}

/**
  * Created by luislazaro on 21/7/17.
  * lalazaro@keedio.com
  * Keedio
  */
class CsvToElastic

object CsvToElastic {

  def main(args: Array[String]): Unit = {

    lazy val properties = new FlinkProperties(args)
    inject(properties)
  }

  def inject(properties: FlinkProperties) = {
    val LOG = Logger.getLogger(classOf[CsvToElastic])

    //use of streaming environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //source
    val dataStreaming: DataStream[String] = env.readTextFile(properties.PATHTOCSVINPUT)
      .name("source readtextFile: " + properties.PATHTOCSVINPUT)

    //transf
    val dataStreamAssessmentDual: DataStream[Either[Assessment, Unit]] = dataStreaming.map(Assessment(_))
      .name("map: parse string to assessment").disableChaining()
    val dataStreamAssessment: DataStream[Assessment] = dataStreamAssessmentDual
      .filter(_.isLeft).name("filter: valid assessments")
      .map(e => e.left.toOption.get).name("map: get assessments")
      .disableChaining()

    //sink
    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", properties.ELASTIC_CLUSTERNAME)
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")
    val transportAddresses = new java.util.ArrayList[TransportAddress]()
    val listOfAddresess: Array[InetSocketTransportAddress] = properties.INET_TRANSPORT_ADDRESSES
    transportAddresses.addAll(java.util.Arrays.asList(listOfAddresess: _*)) match {
      case true => LOG.debug("Added transportAddresses from properties")
      case false => LOG.error("Cannot add transportAddress from properties")
    }

    dataStreamAssessment.addSink(new ElasticsearchSink[Assessment](config, transportAddresses, new
        ElasticsearchSinkFunction[Assessment] {
      def createIndexRequest(element: Assessment): IndexRequest = {
        val json = new java.util.HashMap[String, String]
        json.put("comment", element.comment)
        json.put("overallImpression", element.overallImpression)
        json.put("food", element.food)
        json.put("place", element.place)
        json.put("customerSupport", element.customerSupport)
        json.put("price", element.price)
        json.put("hygiene", element.hygiene)
        json.put("otherStaff", element.otherStaff)

        return Requests.indexRequest()
          .index(properties.INDEX_NAME)
          .`type`(properties.TYPE_NAME)
          .source(json)
      }

      override def process(t: Assessment, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(createIndexRequest(t))
      }
    })).name("sink: ElasticSearch " + properties.ELASTIC_CLUSTERNAME)

    env.execute("Csv To ElasticSearch Injector")

  }
}


