package transformer
import interfaces.Transformer
import interfaces.Injector
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import utility.MTUtils

class Family2Persons extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.{concat, lit}
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    val startTimeMillis = System.currentTimeMillis()

    // --------------- Filters ------------------------------------
    val vertices = gf.vertices
    val edges = gf.edges
    val lstNmFamily = edges.filter($"key" === "lastName")
    val fstNmFamily = edges.filter($"key" === "firstName")

    val lstNmFamilies = lstNmFamily.select($"src", $"dst").join(vertices,
      $"dst" === $"id").select($"src", $"value")

    val fstNmMaleSrcDst = fstNmFamily.select($"src", $"dst").join(vertices
        .filter($"value" === "sons" || $"value" === "father"),
      $"src" === $"id").select($"src", $"dst")

    val fstNmFemaleSrcDst = fstNmFamily.select($"src", $"dst").join(vertices
      .filter($"value" === "daughters" || $"value" === "mother"),
      $"src" === $"id").select($"src", $"dst")

    val fstNmMale = fstNmMaleSrcDst.join(vertices, $"dst" === $"id")
      .select($"src".alias("srcM"), $"value")
    val fstLstNmMale = fstNmMale.join(edges,$"srcM" === $"dst")
      .select($"src", $"value")

    val fstNmFemale = fstNmFemaleSrcDst.join(vertices, $"dst" === $"id")
      .select($"src".alias("srcF"), $"value")

    val fstLstNmFemale = fstNmFemale.join(edges,$"srcF" === $"dst")
      .select($"src", $"value")

    // Transformation Rules Families2Persons -----------------
    val personMale = lstNmFamilies.select($"src".alias("origem"), $"value".alias("lastName"))
    .join(fstLstNmMale, $"src" === $"origem")
    .select(concat($"lastName", lit(" "), $"value") alias "fullName")

    val personFemale = lstNmFamilies.select($"src".alias("origem"), $"value".alias("lastName"))
      .join(fstLstNmFemale, $"src"=== $"origem")
      .select(concat($"lastName", lit(" "), $"value") alias "fullName")

    // Persons union in an only partition............
    val personsDF = personMale.withColumn("Gender", lit("Male"))
      .union(personFemale.withColumn("Gender",lit("Female")))
      .coalesce(1)

    personsDF.write
      .option("rootTag", "persons")
      .option("rowTag", "person")
      .xml("persons.xml")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.00
    println(durationSeconds)

    gf
  }

}
