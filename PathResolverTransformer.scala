package class2relational
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

class PathResolverTransformer extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame =
  {
    import spark.implicits._

    // remember: at this point, the graph is directed and acyclic, like a tree
    // so every vertex has a definite parent vertex (except root of course)

    // 1. identify the vertices that are references to other vertices. Find out their parents and join their IDs in
    // keep the edge keys so that they can be reused later
    val pathVertices = gf.vertices.join(gf.edges, gf.edges("dst") === gf.vertices("id") ).
      filter("(key = \"super\" OR key = \"type\") AND value LIKE \"/%\"").drop("dst").
      withColumnRenamed("src", "parent")

    // 2. flatMap out every path in the vertex's value (there can be multiple, space-separated)
    // returns: id  path_el0  path_el1  parent  edgeKey
    val withParsedPathsDF = pathVertices.flatMap((e: Row) => {

      val the_id: Long = e.getAs(0)
      val the_path: String = e.getAs(1)
      val the_parent: Long = e.getAs(2)
      val the_edgeKey: String = e.getAs(3)

      val paths = the_path.split("\\s+")
      val splitPaths = paths.map( (str: String) => {
        val splat = str.split("/")
        splat
      })

      val ret = splitPaths.map( (splat: Array[String]) => {
        val elems = splat.filter( _.trim != "" )
        try
        {
          (the_id, elems(0).toLong, if(elems.length == 2) (elems(1).split("\\."))(1).toLong else Long.MinValue, the_parent, the_edgeKey)
        }
        catch
        {
          case scala.util.control.NonFatal(t) => {
            System.out.println("Failed to parse split path ("+elems.length+", "+elems(1).split("\\.").length+") : " + elems.reduce((a: String, b:String) => a + " & " + b) + " : " + t.toString)
            (the_id, Long.MinValue, Long.MinValue, the_parent, the_edgeKey)
          }
        }
      })

      ret

    }).toDF("id", "path0", "path1", "final_src", "final_key")

    // 3. create the new edges, from the parent elements of the path value vertices to

    // 3a. create edges that link to data type elements directly
    val datatypeEdgesPreDF = withParsedPathsDF.filter( "path1 == " + Long.MinValue )

    // 3b. create the edges to the datatypes
    val datatypeEdgesDF = datatypeEdgesPreDF.select("final_src", "path0", "final_key").
      withColumnRenamed("final_src", "src").
      withColumnRenamed("path0", "dst").
      withColumnRenamed("final_key", "key")


    // 3c. select parsed paths to classes only
    val pkgClassEdgesPreDF = withParsedPathsDF.filter( "path1 != " + Long.MinValue )

    // 3d. join parsed path to relevant edges from packages to classes
    val pkgClassEdgesFusedDF = pkgClassEdgesPreDF.join( gf.edges, (gf.edges("src") === pkgClassEdgesPreDF("path0")) &&
      (gf.edges("key") === pkgClassEdgesPreDF("path1")) )
    val pkgClassEdgesDF = pkgClassEdgesFusedDF.drop("id", "path0", "path1", "src", "key").
      withColumnRenamed("final_src", "src").
      withColumnRenamed("final_key", "key").
      select("src", "dst", "key")

    // 4. produce final graphframe
    // 4a. final vertices are only the same vertices from before, without the path vertices
    val finalVertices = gf.vertices.join( withParsedPathsDF, Seq("id"), "left_anti" )
    // 4b. final edges are the previous edges with links to path vertices removed, and correct links added
    val finalEdges = gf.edges.join( withParsedPathsDF, $"dst" === $"id", "left_anti" ).
      union(pkgClassEdgesDF).union(datatypeEdgesDF)

    val ret = GraphFrame(finalVertices.persist(StorageLevel.MEMORY_AND_DISK), finalEdges.persist(StorageLevel.MEMORY_AND_DISK))
    //gf.vertices.unpersist()
    //gf.edges.unpersist()
    ret
  }
}
