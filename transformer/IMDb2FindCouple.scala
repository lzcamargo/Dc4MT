package transformer
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

class IMDb2FindCouple extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {

    import spark.implicits._
    val startTimeMillis = System.currentTimeMillis()

    val vertices =  gf.vertices
    val edges =  gf.edges
    
    val qtdeMovie =  edges.filter($"key" === "movies").groupBy($"src").count()
    val actorInMovies =  qtdeMovie.filter($"count" > 2)
    val actorId =  edges.filter($"key" === "movies").select($"src", $"dst")

    // ........ Main Actor Rule ....................
    val mainActorMovie = actorInMovies.select($"src".alias("origem"))
      .join(actorId,$"src" === $"origem")
      .select($"origem", $"dst".alias("destino"))

    val coupleId = mainActorMovie.join(actorId, $"destino" === $"dst")
      .select($"origem", $"destino").distinct()

    // ............ People 1 of the Couple Rule....................
    val people1DF = coupleId.join( edges.filter($"key" === "name"), $"origem" === $"src")
      .select($"origem", $"dst").distinct()
    val people1Name = people1DF.join( vertices, $"dst" === $"id")
      .select($"origem", $"value".alias("name"))

    // ............ People 2 of the Couple Rule....................
    val people2DF = coupleId.join( edges.filter($"key" === "name"), $"destino" === $"dst")
      .select($"origem", $"dst").distinct()
    val people2Name = people2DF.join( vertices, $"dst" === $"id")
      .select($"origem", $"value".alias("name"))

    // ............ Union of People1 and People2 ....................
    val couplesDF = people1Name.union(people2Name).coalesce(1)

    couplesDF.write
      .option("rootTag", "couples")
      .xml("couplesDF.xml")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.000
    println(durationSeconds)

    gf
  }
}