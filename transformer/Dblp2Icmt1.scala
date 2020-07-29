package transformer
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

class Dblp2Icmt1 extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {
    import spark.implicits._
    val startTimeMillis = System.currentTimeMillis()

    val vertices = gf.vertices
    val edges = gf.edges

    // ...... Filters Authors in ICMT with publishing
    val icmtVert = vertices.filter($"value".startsWith("ICMT"))
    val icmtAutorId = icmtVert.join(edges, $"id" === $"dst")
      .select($"id", $"src".alias("origem"))

    // ...... DBLP2ICMT1 Rule ........................
    val icmtAuthor = icmtAutorId.join(edges.filter($"key" === "authors"), $"origem" === $"src")
      .select($"origem", $"dst")
    val icmtAuthorQtde = icmtAuthor.groupBy($"dst").count()
    val icmtAuthorsQtdeDF = icmtAuthorQtde.join(vertices, $"dst" === $"id")
      .select($"value", $"count".alias("Qtde")).distinct().coalesce(1)

    icmtAuthorsQtdeDF.write
      .option("rootTag", "Authors")
      .option("rowTag", "author")
      .xml("authorQtde.xml")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.000
    println(durationSeconds)

    gf
  }
}