package utility

import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object MTUtils {

  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.map(_._1)
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
  }

  def currentExecutorCount(sc: SparkContext): Int = {
    currentActiveExecutors(sc).length
  }

  def getLogger() : Logger = {
    Logger.getLogger("MT")
  }

}

object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}