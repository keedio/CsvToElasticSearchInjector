package com.keedio.kds.flink.mappers

import java.io.StringReader

import com.keedio.kds.flink.models.Assessment
import com.opencsv.CSVReader
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter

/**
  * Created by luislazaro on 26/7/17.
  * lalazaro@keedio.com
  * Keedio
  */
class AssessmentMapFunction extends RichMapFunction[String, Assessment]{

  var counter: Counter = _

  @throws(classOf[Exception])
  override def map(s: String): Assessment = {
    val reader = new CSVReader(new StringReader(s), ',', '"')
    new Assessment(reader.readNext(): _*)
  }

  override def open(config: Configuration) = {
  this.counter = getRuntimeContext
      .getMetricGroup
      .addGroup("MyMetrics")
      .counter("StringToAssessment_counter")
  }

}