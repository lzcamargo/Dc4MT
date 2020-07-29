package core

import java.nio.file.{Files, Paths}

import interfaces.{Injector, Transformer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import utility.MTUtils

import scala.io.Source



object Main {

  val CONFIG_FILENAME = "conf/mt-spark.conf"

  var spark : SparkSession = null
  var ctx : SparkContext = null

  def main(args: Array[String]) {

    val conf = new SparkConf(true )
    conf.setAppName("MT")

    System.out.println("MT working dir: " + (new java.io.File(".").getCanonicalPath))
    loadConfigs(conf)

    for (arg <- args) {
      if (arg.count(_ == '=') == 1) {
        val splat = arg.split("=")
        if (splat.length == 2) {
          conf.set(splat(0), splat(1))
          System.out.println("ARG: setting " + splat(0) + " to " + splat(1))
        }
        else System.err.println("WARNING: ignoring crappy argument " + arg)
      }
      else System.err.println("WARNING: ignoring crappy argument " + arg)
    }

    if(conf.get("spark.master", "").isEmpty)
      conf.setMaster("local[8]")

    this.spark = SparkSession.builder.config(conf).getOrCreate()
    this.ctx = spark.sparkContext

    val injectorClass = conf.get("injector.class", "injector.TestInjector")
    val injector = Class.forName(injectorClass).newInstance().asInstanceOf[Injector]
    var gf: GraphFrame = injector.inject(spark)

    gf.edges.rdd.setName("origEdges")
    gf.vertices.rdd.setName("origVertices")

    val transformerCount = conf.get( "transformer.count", "0").toInt
    for (tidx <- 0 until transformerCount )
    {
      val transformerClass = conf.get("transformer" + tidx.toString + ".class", "transformer.DummyTransformer")
      val transformer = Class.forName(transformerClass).newInstance().asInstanceOf[Transformer]
      gf = transformer.transform(spark, gf)
    }

  }

  def loadConfigs( sc: SparkConf ) : Unit =
  {
    var filename = CONFIG_FILENAME

    var priority_config: Boolean = Files.exists(Paths.get("./mt-scala.conf"))
    if(priority_config)
    {
      filename = "./mt-scala.conf"
      System.out.println("USING CONFIG \"./mt-scala.conf\"")
    }


    if(Files.exists(Paths.get(filename)))
      for (line <- Source.fromFile(filename).getLines) {
        val tokens = line.split("\\s+")
        if(tokens.length == 2)
          sc.set(tokens(0), tokens(1))
        else
          MTUtils.getLogger().warn("skipped weird configuration line: " + line)
      }
    else
      System.out.println("NO CONFIG FILE: \"" + filename + "\", skipping...")


  }

}