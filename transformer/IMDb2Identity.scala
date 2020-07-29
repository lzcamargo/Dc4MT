package transformer
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

class IMDb2Identity extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {

    import spark.implicits._
    val startTimeMillis = System.currentTimeMillis()

    val vertices = gf.vertices
    val edges = gf.edges

    //Movie Filter--------------------------------
    val verticesMovie =  vertices.filter($"value" === "ns1:Movie")
    val moviesId = verticesMovie.join( edges,$"src" === $"id")
      .select($"id".alias("movid"),$"key", $"dst" )

    //Actor Filter--------------------------------
    val verticesActor =  vertices.filter($"value" === "ns1:Actor")
    val actorId = verticesActor.join( edges,$"src" === $"id")
      .select($"id".alias("actid"), $"dst", $"key")

    //Actress Filter--------------------------------
    val verticesActress =  vertices.filter($"value" === "ns1:Actress")
    val actressId = verticesActress.join( edges,$"src" === $"id")
      .select($"id".alias("actid"), $"dst", $"key")

    //  .............. Movie Rule ................
    val moviesDF = moviesId.join( vertices, $"dst" === $"id")
      .select($"movid", $"key", $"value")

    // ............. Actor Rule .......................
    val actorName = actorId.filter($"key" === "name").join(vertices, $"dst" === $"id")
      .select($"actid".alias("actidn"), $"value".alias("name"))
    val actorMovieDF =  actorId.filter($"key" === "movies")
      .select($"actid", $"dst", $"key".alias("movies")).distinct()

        //........ Actress Rule ...........
    val actressName = actressId.filter($"key" === "name").join(vertices, $"dst" === $"id")
      .select($"actid".alias("actidn"), $"value".alias("name"))
    val actressMovieDF =  actressId.filter($"key" === "movies")
      .select($"actid", $"dst", $"key".alias("movies")).distinct()

    //Transformation Output
    val movieActor = moviesDF.join(actorMovieDF, $"movid" === $"dst")
      .select($"movid", $"value", $"actid").distinct()
    val outMovieActor = movieActor.join(actorName, $"actidn" === $"actid")
      .select($"movid", $"value", $"actid", $"name").distinct()
    val movieActress = moviesDF.join(actressMovieDF, $"movid" === $"dst")
      .select($"movid", $"value", $"actid").distinct()
    val outActressActor = movieActress.join(actorName, $"actidn" === $"actid")
      .select($"movid", $"value", $"actid", $"name").distinct()

    val identyDF = outMovieActor.union(movieActress).coalesce(1)

    identyDF.write
     .option("rootTag", "movies")
     .xml("identity.xml")

   val endTimeMillis = System.currentTimeMillis()
   val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.000
    println(durationSeconds)

    gf

  }
}
