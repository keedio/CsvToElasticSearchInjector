package com.keedio.kds.flink.injector.models

import java.io.StringReader

import com.opencsv.CSVReader
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

  /**
    * Parse a string via CSVReader. The result is a reader that will be the argument for building
    * an Assessment instance.
    * If the number of fields produced by CSVReader is different from the number of args for
    * building an Assessment, the string parsed is no valid and error will be logged.
    * CSVReader may throw IOException if any other staff happens when trying to parse.
    * Apply returns an Either object, with Left as valid Assessments and Right as Unit.
    * @param s
    * @param separator
    * @param quotechar
    * @return
    */
  def apply(s   : String, separator: Char = ',', quotechar: Char = '"'): Either[Assessment, Unit] = {
    val reader = new CSVReader(new StringReader(s), separator, quotechar)
    val parsedFields: Array[String] = reader.readNext()
    parsedFields.size == classOf[Assessment].getDeclaredConstructors()(0).getParameterCount match {
      case true => Left(new Assessment(parsedFields: _*))
      case false => Right(LOG.error("String: " + "\"" + s + "\"" + " with " + parsedFields.size +
        " fields" + " cannot be parsed to " + this.getClass.getName + " object"))
    }
  }

}