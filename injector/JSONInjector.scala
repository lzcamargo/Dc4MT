package injector

import java.io.{FileInputStream, FileOutputStream}
import java.net.URI
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator, JsonParser, JsonToken}
import interfaces.Injector
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import utility.MTUtils

import scala.collection.mutable.ArrayBuffer


class JSONInjector extends Injector {

  type NodeEntryPre = (Long, String, Long, String) // a node with id, value, parent id, and parent edge key


  def ensureModelSpliced(path: String, boundaryTag: String, rootTag: String): String = {

    val splicePath = path + ".spliced/"

    if (Files.exists(Paths.get(splicePath))) {
      System.out.println("Model already spliced, skipping: " + splicePath)
      return splicePath
    }

    System.out.println("Splicing model to " + splicePath)

    // BEGIN SPLICE JSON
    Files.createDirectories(Paths.get(splicePath))

    val in_stream = new FileInputStream(path)
    val fac = new JsonFactory()
    val in_parser : JsonParser = fac.createParser(in_stream)


    var token = in_parser.nextToken
    // find first array, containing the top-level objects
    while ( {!JsonToken.START_ARRAY.equals(token) })
      token = in_parser.nextToken

    if (token == null) return ""
    var depth = 0
    var curr_idx = 0

    var curr_gen: JsonGenerator = null
    var elems_written = 0

    class CounterOutputStream(val fname: String) extends java.io.OutputStream
    {
      var bytesWritten: Int = 0
      val underling: FileOutputStream = new FileOutputStream(fname)
      override def write(i: Int): Unit = {underling.write(i); bytesWritten += 1}
      override def write(bytes: Array[Byte]): Unit = {underling.write(bytes); bytesWritten += bytes.length}
      override def write(bytes: Array[Byte], offset: Int, len: Int): Unit = {underling.write(bytes, offset, len); bytesWritten += len}
      override def flush(): Unit = underling.flush()
    }
    var out : CounterOutputStream = null

    // we are on the top-level array now
    while(!in_parser.isClosed()){


      val prev_depth = depth
      val token : JsonToken = in_parser.nextToken()

      if (token != null)
        token match {
          case JsonToken.NOT_AVAILABLE => return ""
          case JsonToken.START_OBJECT => {
            depth += 1;
            if(depth == 1)
            {
              // a top-level object, let's stream it out
              if(curr_gen == null)
              {
                out = new CounterOutputStream(splicePath + curr_idx)
                curr_gen = fac.createGenerator(out)
                curr_gen.useDefaultPrettyPrinter()
                curr_gen.writeStartArray()
              }

              System.out.println("WRITING AN OBJECT")
              curr_gen.copyCurrentStructure(in_parser)
              curr_idx += 1
              depth -= 1
              elems_written += 1;

              if(out.bytesWritten > 1 * 1024 * 1024)
              {
                curr_gen.writeEndArray()
                curr_gen.close()
                curr_gen = null

                out.close()
                out = null
                elems_written = 0
              }
            }
          }
          case JsonToken.END_OBJECT => {depth -= 1;}
          case JsonToken.START_ARRAY => {depth += 1;}
          case JsonToken.END_ARRAY => {depth -= 1;}
          case JsonToken.FIELD_NAME =>
          case JsonToken.VALUE_EMBEDDED_OBJECT =>
          case JsonToken.VALUE_STRING =>
          case JsonToken.VALUE_NUMBER_INT =>
          case JsonToken.VALUE_NUMBER_FLOAT =>
          case JsonToken.VALUE_TRUE =>
          case JsonToken.VALUE_FALSE =>
          case JsonToken.VALUE_NULL => {} // ignore, most likely the end
        }
        System.out.println("jsonToken = " + token, ", depth=" + depth.toString)
    }

    if(curr_gen != null)
    {
      curr_gen.writeEndArray()
      curr_gen.close()
      curr_gen = null
    }

    // END SPLICE JSON

    System.out.println("Done splicing model to " + splicePath)

    return splicePath
  }


  @Override
  override def inject(spark: SparkSession): GraphFrame = {
    import spark.implicits._

    val conf = spark.conf
    val modelPath = conf.get("splicedxmlinjector.modelpath")
    val boundaryTag = conf.get("splicedxmlinjector.boundarytag")
    val rootTag = conf.get("splicedxmlinjector.roottag")

    val splicePath = ensureModelSpliced(modelPath, boundaryTag, rootTag)

    val toHDFS = conf.get("splicedxmlinjector.hdfsupload").toBoolean

    val finalPath = if (toHDFS) "hdfs:///model-in/" else splicePath

    if (toHDFS) {
      val srcFileSystem: FileSystem = FileSystem.getLocal(spark.sparkContext.hadoopConfiguration)
      val dstFileSystem: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      FileUtil.copy(
        srcFileSystem,
        new Path(new URI(splicePath)),
        dstFileSystem,
        new Path(new URI(finalPath)),
        true,
        spark.sparkContext.hadoopConfiguration)
    }

    // INÍCIO EXTRAÇÃO

    val filesRDD = spark.sparkContext.wholeTextFiles(splicePath, spark.sparkContext.defaultParallelism)
    val nodesRDD = filesRDD.repartition(100).flatMap((el: (String, String)) => {

      var initialIndex: Long = el._1.split("/").last.toLong

      val tc: TaskContext = TaskContext.get()
      val currPartition: Long = tc.partitionId()
      var currIdGen: Long = (initialIndex + 1) << 32
      val logm = "XMLInjector.Inject: parseXML on partition " + currPartition.toString + " with index base " + initialIndex + ": " + el._1
      MTUtils.getLogger().info(logm)

      val ret = new ArrayBuffer[NodeEntryPre]()
      val idstack = new ArrayBuffer[Long]() // stack of ids of the current hierarchy
      var currElemIdx: Long = 0

      ret
    })

    val nodesRDDPersisted = nodesRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // extract edges from nodes rdd
    // at this point the root node is not present on the elements, so they all
    // have an edge towards their parent
    val edgesDF = nodesRDDPersisted.toDF("dst", "dropped", "src", "key").drop("dropped")

    // root node to be added
    val rootVertexRDD = spark.sparkContext.parallelize(List(new NodeEntryPre(-1, "root", Long.MinValue, "")))

    // convert nodes rdd to a DataFrame with proper column names, and drop edge info
    // we mark the resulting RDD as persistent on memory and disk, so that the
    // extraction result gets cached properly
    val verticesDF = (rootVertexRDD ++ nodesRDD).toDF("id", "value", "parent", "pkey").
      drop("parent", "pkey")

    // return the resulting graphframe
    GraphFrame(verticesDF.repartition(100).persist(StorageLevel.MEMORY_AND_DISK),
      edgesDF.repartition(100).persist(StorageLevel.MEMORY_AND_DISK))
  }

}
