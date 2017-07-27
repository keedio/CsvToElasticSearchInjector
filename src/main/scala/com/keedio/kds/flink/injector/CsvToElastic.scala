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
    val LOG = Logger.getLogger(classOf[CsvToElastic])
    lazy val properties = new FlinkProperties(args)
    inject(properties)
  }

  def inject(properties: FlinkProperties) = {

      //use of streaming environment
      val envStreaming: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      envStreaming.setParallelism(1)
      //source
      val dataStreaming: DataStream[String] = envStreaming.readTextFile(properties.PATHTOCSVINPUT)

      //transf
      val dataStreamAssessmentDual: DataStream[Either[Assessment, Unit]] = dataStreaming.map(Assessment(_))
      val dataStreamAssessment: DataStream[Assessment] = dataStreamAssessmentDual
        .filter(_.isLeft)
        .map(e => e.left.toOption.get)
      dataStreamAssessment.map(e => println(e))


      //sink
      val config = new java.util.HashMap[String, String]
      config.put("cluster.name", "KDS_Seman")
      // This instructs the sink to emit after every element, otherwise they would be buffered
      config.put("bulk.flush.max.actions", "1")
      val transportAddresses = new java.util.ArrayList[TransportAddress]()
      transportAddresses.add(new InetSocketTransportAddress("ambari1.ambari.keedio.org", 9300))
      transportAddresses.add(new InetSocketTransportAddress("ambari2.ambari.keedio.org", 9300))
      transportAddresses.add(new InetSocketTransportAddress("ambari3.ambari.keedio.org", 9300))

      dataStreamAssessment
        .addSink(new ElasticsearchSink[Assessment](config, transportAddresses, new
            ElasticsearchSinkFunction[Assessment] {

          def createIndexRequest(element: Assessment): IndexRequest = {
            val json = new java.util.HashMap[String, String]
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


