package interfaces

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

abstract class Transformer {

  def transform( spark: SparkSession, gf: GraphFrame ): GraphFrame

}
