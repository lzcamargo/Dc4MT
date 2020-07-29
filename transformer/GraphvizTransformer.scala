package transformer
import java.awt.Desktop
import java.io.FileOutputStream

import guru.nidi.graphviz.attribute.{Color, Label}
import guru.nidi.graphviz.engine.{Engine, Format, Graphviz}
import guru.nidi.graphviz.model.MutableGraph
import org.apache.spark.sql.{Row, SparkSession}
import org.graphframes.GraphFrame
import utility.MTUtils

class GraphvizTransformer extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame =
  {
    import java.io.File
    val file: File = File.createTempFile("mt-scala-viz-", ".dot")
    MTUtils.getLogger.info("[GraphvizTransformer] outputting to " + file.getAbsolutePath)
    file.deleteOnExit()

    val fos: FileOutputStream = new FileOutputStream(file)

    val mg : MutableGraph = guru.nidi.graphviz.model.Factory.mutGraph("mt-scala visualization")
    mg.setDirected(true)

    val collected = gf.edges.collect()

    MTUtils.getLogger().info("[graphviz] collected " + collected.length.toString + " edges")

    collected.foreach((r: Row) => {
      val src = r.getAs[Long]("src")
      val dst = r.getAs[Long]("dst")
      val key = r.getAs[String]("key")

      import guru.nidi.graphviz.attribute.Label
      import guru.nidi.graphviz.model.Factory.{mutNode, to}
      val typeOrSuper: Boolean = key == "type" || key == "super"
      if(!typeOrSuper)
        mg.add(mutNode(src.toString).addLink(to(mutNode(dst.toString)).`with`(Label.of(key.toString))))
      else
        mg.add(mutNode(src.toString).addLink(to(mutNode(dst.toString)).`with`(Label.of(key.toString)).`with`(Color.BLUE)))
    })
    mg.add( guru.nidi.graphviz.model.Factory.mutNode("-1").add(Color.RED) )

    gf.vertices.collect().foreach((r: Row) => {
      val id = r.getAs[Long]("id")
      val value = r.getAs[String]("value")
      mg.add(
        guru.nidi.graphviz.model.Factory.mutNode(id.toString).add("label", value)
      )

    })

    mg.graphAttrs().
      add("overlap", "false")

    mg.graphAttrs().
      add("splines", "true")

    val dest = new File("/tmp/mt-scala-viz"+System.currentTimeMillis().toString+".svg")
    Graphviz.fromGraph(mg).fontAdjust(0.75).engine(Engine.NEATO).render(Format.SVG).toFile(dest)
    if(dest.exists())
    {
      Desktop.getDesktop.open(dest)
      dest.deleteOnExit()
    }

    gf
  }
}
