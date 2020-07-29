package transformer

import interfaces.Transformer
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import utility.MTUtils

class DummyTransformer extends Transformer {

  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {


    MTUtils.getLogger().info("Graph vertices:")
    gf.vertices.show(100,false)

    MTUtils.getLogger().info("Graph edges:")
    gf.edges.show(100, false)
/*
    MTUtils.getLogger().info("Graph inDegrees:")
    gf.inDegrees.show()

    MTUtils.getLogger().info("Graph outDegrees:")
    gf.outDegrees.show()

    MTUtils.getLogger().info("Graph triplets:")
    gf.triplets.show()
    */
    gf

  }
 }
