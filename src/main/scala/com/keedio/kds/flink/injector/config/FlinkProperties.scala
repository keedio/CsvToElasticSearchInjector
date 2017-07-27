package com.keedio.kds.flink.injector.config

import com.keedio.kds.flink.injector.utils.InyectorHelper
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.JavaConverters._
/**
  * Created by luislazaro on 17/5/17.
  * lalazaro@keedio.com
  * Keedio
  */
class FlinkProperties(args: Array[String]) extends Serializable {
  require(!args.isEmpty)
  lazy val parameterToolCli: ParameterTool = ParameterTool.fromArgs(args)
  lazy val parameterToolFromFile: ParameterTool = parameterToolCli.getProperties.propertyNames().asScala.toSeq.contains("properties.file") match {
    case true => ParameterTool.fromPropertiesFile(parameterToolCli.get("properties.file"))
    case false => parameterToolCli
  }

    lazy val PATHTOCSVINPUT = InyectorHelper.getValueFromProperties(parameterToolCli, "input.csv",
      InyectorHelper.getValueFromProperties(parameterToolFromFile, "input.csv", ""))

    lazy val SEPARATOR = InyectorHelper.getValueFromProperties(parameterToolCli, "field.separator",
      InyectorHelper.getValueFromProperties(parameterToolFromFile, "field.separator", ",")).asInstanceOf[Char]

    lazy val QUOTECHAR = InyectorHelper.getValueFromProperties(parameterToolCli, "field.quoter",
      InyectorHelper.getValueFromProperties(parameterToolFromFile, "field.quoter", ",")).asInstanceOf[Char]


    //properties form cli will override properties from file. Elemenst from right operand owerwrite elemenst of
    //left one
    lazy val mapOfParameters = parameterToolFromFile.getProperties.asScala.toSeq.toMap ++
      parameterToolCli.getProperties.asScala.toSeq.toMap
    lazy val parameterTool = ParameterTool.fromMap(mapOfParameters.asJava)

   }