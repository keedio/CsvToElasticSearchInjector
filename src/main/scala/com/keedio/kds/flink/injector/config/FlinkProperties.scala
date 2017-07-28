package com.keedio.kds.flink.injector.config

import com.keedio.kds.flink.injector.utils.InyectorHelper
import org.apache.flink.api.java.utils.ParameterTool
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.collection.JavaConverters._

/**
  * Created by luislazaro on 17/5/17.
  * lalazaro@keedio.com
  * Keedio
  */
class FlinkProperties(args: Array[String]) extends Serializable {
  require(!args.isEmpty)
  lazy val parameterToolCli: ParameterTool = ParameterTool.fromArgs(args)
  lazy val parameterToolFromFile: ParameterTool = parameterToolCli.getProperties.propertyNames().asScala.toSeq
    .contains("properties.file") match {
    case true => ParameterTool.fromPropertiesFile(parameterToolCli.get("properties.file"))
    case false => parameterToolCli
  }

  //properties form cli will override properties from file. Elemenst from right operand owerwrite elemenst of
  //left one
  lazy val mapOfParameters = parameterToolFromFile.getProperties.asScala.toSeq.toMap ++
    parameterToolCli.getProperties.asScala.toSeq.toMap

  lazy val parameterTool: ParameterTool = ParameterTool.fromMap(mapOfParameters.asJava)

  lazy val PATHTOCSVINPUT = InyectorHelper.getValueFromProperties(parameterTool, "input.csv",
    InyectorHelper.getValueFromProperties(parameterToolFromFile, "input.csv", ""))

  lazy val SEPARATOR = InyectorHelper.getValueFromProperties(parameterTool, "field.separator",
    InyectorHelper.getValueFromProperties(parameterToolFromFile, "field.separator", ",")).asInstanceOf[Char]

  lazy val QUOTECHAR = InyectorHelper.getValueFromProperties(parameterTool, "field.quoter",
    InyectorHelper.getValueFromProperties(parameterToolFromFile, "field.quoter", "\"")).asInstanceOf[Char]

  lazy val ELASTIC_CLUSTERNAME = InyectorHelper.getValueFromProperties(parameterTool, "cluster.name",
    InyectorHelper.getValueFromProperties(parameterToolFromFile, "cluster.name", ""))

  lazy val INDEX_NAME = InyectorHelper.getValueFromProperties(parameterTool, "index.name",
    InyectorHelper.getValueFromProperties(parameterToolFromFile, "index.name", ""))

  lazy val TYPE_NAME = InyectorHelper.getValueFromProperties(parameterTool, "type.name",
    InyectorHelper.getValueFromProperties(parameterToolFromFile, "type.name", ""))

  lazy val INET_TRANSPORT_ADDRESSES: Array[InetSocketTransportAddress] = InyectorHelper
    .getValueFromProperties(parameterTool, "inet.addresses.rpc",
      InyectorHelper.getValueFromProperties(parameterToolFromFile, "inet.addresses.rpc", "localhost,9300"))
    .split(";")
    .map(transportAddress => transportAddress.trim.split(","))
    .map(arrTAddress => new InetSocketTransportAddress(arrTAddress(0).trim, arrTAddress(1).trim.toInt))

}