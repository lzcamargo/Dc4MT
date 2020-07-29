package interfaces

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

abstract class Injector extends Serializable {

  def inject( spark: SparkSession ) : GraphFrame

}
