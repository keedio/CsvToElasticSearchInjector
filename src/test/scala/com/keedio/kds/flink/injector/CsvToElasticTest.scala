package com.keedio.kds.flink.injector

import com.keedio.kds.flink.injector.config.FlinkProperties
import org.junit.Test

/**
  * Created by luislazaro on 27/7/17.
  * lalazaro@keedio.com
  * Keedio
  */
private[injector] class CsvToElasticTest {
  @Test
  def executeInjector()= {
    val args: Array[String] = Array(
      "--input.csv",  "./src/test/resources/kdsMexicanFood.csv",
      "--cluster.name", "KDS_Seman",
      "--index.name", "assessments",
      "--type.name", "assessment",
      "--inet.addresses.rpc", "ambari1.ambari.keedio.org, 9300; ambari2.ambari.keedio.org, 9300; ambari3.ambari.keedio.org, 9300"
    )

    val properties = new FlinkProperties(args)
    CsvToElastic.inject(properties)
    println
   }

  @Test
  def executeInjectorFromPropertiesFile() = {
    val args = Array("--properties.file", "./src/test/resources/flinkjob_injector.properties")
    val properties = new FlinkProperties(args)
    CsvToElastic.inject(properties)
    println

  }

}
