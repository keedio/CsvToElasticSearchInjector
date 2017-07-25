package com.keedio.kds.flink.models

import org.apache.log4j.Logger

/**
  * Created by luislazaro on 24/7/17.
  * lalazaro@keedio.com
  * Keedio
  */
class Assessment(
                  val comment: String,
                  val generalFeeling: String,
                  val food: String,
                  val place: String,
                  val customerSupport: String,
                  val price: String,
                  val sanitation: String,
                  val otherStaff: String
                ) {

  def this() = this("", "", "", "", "", "", "", "")

  def this(args: String*) = this(args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))

  override def toString = {
    new String(
      s"""$comment,$generalFeeling,$food,$place,$customerSupport,$price,$sanitation,$otherStaff""")
  }
}

object Assessment extends Serializable {

  val LOG = Logger.getLogger(classOf[Assessment])

  def apply(s: String): Assessment = {
    s.startsWith("\"") match {
      case true => new Assessment(s.split("\",", -1)(0).replace("\"", "") +: s.split("\",", -1)(1).split(",", -1)
        .toSeq: _*)
      case false => new Assessment(s.split(",", -1): _*)
    }
  }

}