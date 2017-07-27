package com.keedio.kds.flink.injector.utils

import org.apache.flink.api.java.utils.ParameterTool




/**
  * Created by luislazaro on 27/2/17.
  * lalazaro@keedio.com
  * Keedio
  */
object InyectorHelper {

  /**
    * Check for not mandatory key in parametertool map. If not exists,  provide default value for such a key.
    * If key exists, check for value and return provided value or default.
    * Method for pairs key-vals in map parametertool.
    *
    * @param parameterTool
    * @param key
    * @param default
    * @return
    */
  def getValueFromProperties(parameterTool: ParameterTool, key: String, default: String): String = {
    parameterTool.has(key) match {
      case true => Option(parameterTool.get(key)) match {
        case Some(value) => value match {
          case "" => default match {
            case "" => new RuntimeException(s"Properties key ${key} cannot be empty")
              ""
            case _ => default
          }
          case _ => value
        }
        case None => default
      }
      case false => default
    }
  }

}
