package transformer
import interfaces.Transformer
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import utility.MTUtils

// -------------- transformation of the Class10-5-0 model ------------

class Class02Relational extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    spark.conf.set("spark.sql.crossJoin.enabled", true)
    val startTimeMillis = System.currentTimeMillis()

    // --------------------Package Filters ---------------------
    //package filter: select the src and dst of all Packages for indicating their names
    val packages = gf.vertices.filter($"value" === "classmm:Package")

    val srcDstPackages = packages.join(gf.edges.filter($"key" === "name"), $"id" === $"src")
      .select($"src",$"dst")

    //--------------------Class Filters---------------------------------
    val classes = gf.vertices.filter($"value" === "classes")
    val classAbstract = classes.join(gf.edges.filter($"key" === "isAbstract"), $"id" === $"src")
        .select($"src", $"dst", $"key")
    val classAbstractId = classAbstract.join(gf.vertices.filter($"value" === "false"), $"dst" === $"id")
      .select($"src".alias("origem"))

    // Package2Schema Rule
    val schemaDF = srcDstPackages.join(gf.vertices,$"id" === $"dst")
      .select($"src", $"value".alias("name"))

    // Class2Schema Rule
    val classIdName = classAbstractId.join(gf.edges.filter($"key" === "name"),$"origem" === $"src")
      .select($"origem", $"dst")
    val tableDF = classIdName.join(gf.vertices,$"dst" === $"id")
      .select($"origem", $"value".alias("name"))

    val relationalDF = schemaDF.union(tableDF)
    relationalDF.show(100)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.000
    println(durationSeconds)
    gf
  }
}