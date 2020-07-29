package transformer

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

class Dblp2Icmt2 extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val startTimeMillis = System.currentTimeMillis()

    val vertices = gf.vertices
    val edges = gf.edges
    // ...... Filters Authors in ICMT with publishing

    val icmtVert = vertices.filter($"value".startsWith("ICMT"))
    val icmtAutorId = icmtVert.join(edges, $"id" === $"dst")
      .select($"id", $"src".alias("origem"))
    val icmtAuthor = icmtAutorId.join(edges.filter($"key" === "year"), $"origem" === $"src")
      .select($"origem", $"dst")
    val icmtAuthorYear = icmtAuthor.join(vertices, $"dst" === $"id")
      .select($"origem", $"value")
    val yearBase = edges.filter($"key" === $"year").
      join(vertices, $"dst" === $"id")
      .agg(max($"value"))

    // ...... Dblp2Icmt2 Rule ........................

    val icmtAuthorsActive =  icmtAuthorYear.when($"value".geq ('yearBase - 5))
      .select($"origem")
    val icmtAuthorsInactive = icmtAuthorYear.when($"value".lt ('yearBase - 5))
      .select($"origem")

    val authorActiveDF = icmtAuthorsActive.join(edges
      .filter($"key" === "name" ), $"origem" === $"src").select($"dst")
      .join(vertices, $"dst" === $"id")
      .select($"value").withColumn("Active", lit("yes")).distinct()

    val authorInactiveDF = icmtAuthorsActive.join(edges
      .filter($"key" === "name" ), $"origem" === $"src").select($"dst")
      .join(vertices, $"dst" === $"id")
      .select($"value").withColumn("Active", lit("no")).distinct()

    val authorsDF = authorActiveDF.union(authorInactiveDF).colasce(1)
    authorsDF.write
      .option("rootTag", "Authors")
      .option("rowTag", "author")
      .xml("authorsActInt.xml")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.000
    println(durationSeconds)

    gf
  }
}