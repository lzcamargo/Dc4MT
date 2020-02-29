import java.io.{BufferedReader, File, FileInputStream, FileOutputStream, IOException, StringReader}
import java.nio.file.{Files, Paths}
import java.net.URI

import interfaces.Injector
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}
import org.apache.commons.io.FileSystemUtils
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame
import utility.MTUtils

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import util.control.Breaks._

class SplicedXMLInjector extends Injector {
  val startTimeMillis = System.currentTimeMillis()
  type NodeEntryPre = (Int, String, Int, String) // a node with id, value, parent id, and parent edge key


  def ensureModelSpliced(path: String, boundaryTag: String, rootTag: String): String = {

    val splicePath = path + ".spliced/"

    if (Files.exists(Paths.get(splicePath))) {
      System.out.println("Model already spliced, skipping: " + splicePath)
      return splicePath
    }
    val startTimeMillis = System.currentTimeMillis()
    System.out.println("Splicing model to " + splicePath)

    Files.createDirectories(Paths.get(splicePath))
    var currOut: FileOutputStream = null
    var currIndex: Int = -1
    var currWritten: Long = 0
    val blockMin: Long = (1024 * 1024 * 32) // 32mb blocks

    val openRoot = (stream: FileOutputStream) => {
      stream.write("<_inj_root>".getBytes)
    }
    val closeRoot = (stream: FileOutputStream) => {
      stream.write("</_inj_root>".getBytes)
    }


    for (line <- Source.fromFile(path).getLines()) {
      if ((!rootTag.isEmpty) && !line.contains(rootTag)) {


        val boundaryLine = line.contains(boundaryTag)
        val terminatorLine = line.contains("</") || line.contains("/>")
        val oneLiner = line.contains("/>") && !line.contains("</")

        if (boundaryLine) {
          if (terminatorLine) {

            if(oneLiner)
              currIndex += 1

            if (currOut == null) {
              currOut = new FileOutputStream(splicePath + currIndex.toString)
              openRoot(currOut)
            }

            val bs: Array[Byte] = line.getBytes()
            currOut.write(bs)
            currWritten += bs.length

            if (currWritten > blockMin) {
              closeRoot(currOut)
              currOut.close()
              currOut = null
              currWritten = 0
            }

          }
          else {
            // boundary line but not terminator, start of boundary
            currIndex += 1

            if (currOut == null) {
              currOut = new FileOutputStream(splicePath + currIndex.toString)
              openRoot(currOut)
            }


            val bs: Array[Byte] = line.getBytes()
            currOut.write(bs)
            currWritten += bs.length

          }
        }
        else if (currOut != null) {
          // just part of an existing block
          val bs: Array[Byte] = line.getBytes()
          currOut.write(bs)
          currWritten += bs.length
        }

      } // if not root tag
    } // end for

    if (currOut != null) {
      closeRoot(currOut)
      currOut.close()
    }

    System.out.println("Done splicing model to " + splicePath)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.00
    println(durationSeconds)

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

    val filesRDD = spark.sparkContext.wholeTextFiles(splicePath, spark.sparkContext.defaultParallelism)
    val nodesRDD = filesRDD.repartition(100).flatMap((el: (String, String)) => {

      var initialIndex = el._1.split("/").last.toInt

      val tc: TaskContext = TaskContext.get()
      val currPartition: Long = tc.partitionId()
      var currIdGen = (initialIndex + 1) << 20
      val logm = "XMLInjector.Inject: parseXML on partition " + currPartition.toString + " with index base " + initialIndex + ": " + el._1
      MTUtils.getLogger().info(logm)

      val ret = new ArrayBuffer[NodeEntryPre]()
      val idstack = new ArrayBuffer[Long]() // stack of ids of the current hierarchy
      var currElemIdx: Int = 0

      val fac: XMLInputFactory = XMLInputFactory.newInstance()
      fac.setProperty("javax.xml.stream.isNamespaceAware", false)
      val rdr: XMLStreamReader = fac.createXMLStreamReader(new StringReader(el._2))

      if (rdr.next != XMLStreamConstants.START_ELEMENT)
        throw new IOException("Invalid XML split found")

      while (rdr.hasNext) {
        val evt = rdr.next
        evt match {
          case XMLStreamConstants.START_ELEMENT => {
            val elname = rdr.getName().toString

            if (idstack.isEmpty) { // sub-root element

              ret += new NodeEntryPre(initialIndex, elname, -1, initialIndex.toString)
              idstack += initialIndex

            } else {

              ret += new NodeEntryPre(currIdGen.toInt, elname, idstack.last.toInt,
                if (idstack.length <= 2) currElemIdx.toString else elname)
              idstack += currIdGen

              if (idstack.length == 2)
                currElemIdx += 1

              currIdGen += 1
            }

            val attrCount = rdr.getAttributeCount()
            if (attrCount != 0) {
              for (i <- 0 to (attrCount - 1)) {
                val attrid = rdr.getAttributeName(i)
                val attrval = rdr.getAttributeValue(i)
                //System.out.println( "XMLInjector.Inject: Attribute: " + attrid + "->" + attrval );
                ret += new NodeEntryPre(currIdGen.toInt, attrval, idstack.last.toInt, attrid.toString)
                currIdGen += 1
              }
            }
          }

          case XMLStreamConstants.END_ELEMENT => {
            if (!idstack.isEmpty)
              idstack.remove(idstack.length - 1)

            if (idstack.isEmpty) {
              currElemIdx = 0
              initialIndex += 1
            }
          }
          case XMLStreamConstants.PROCESSING_INSTRUCTION => {

          }
          case XMLStreamConstants.CHARACTERS => {

          }
          case XMLStreamConstants.COMMENT => {

          }
          case XMLStreamConstants.SPACE => {

          }
          case XMLStreamConstants.START_DOCUMENT => {

          }
          case XMLStreamConstants.END_DOCUMENT => {
            //System.out.println("XMLInjector.Inject: End Document?");
          }
          case XMLStreamConstants.ENTITY_REFERENCE => {

          }
          case XMLStreamConstants.ATTRIBUTE => {
            //System.out.println("XMLInjector.Inject: Attribute?");
          }
          case XMLStreamConstants.DTD => {
            //System.out.println("XMLInjector.Inject: DTD?");
          }
          case XMLStreamConstants.CDATA => {
            //System.out.println("XMLInjector.Inject: CDATA?");
          }
          case XMLStreamConstants.NAMESPACE => {
            //System.out.println("XMLInjector.Inject: Namespace?");
          }
          case XMLStreamConstants.NOTATION_DECLARATION => {
            //System.out.println("XMLInjector.Inject: Notation declaration?");
          }
          case XMLStreamConstants.ENTITY_DECLARATION => {
            //System.out.println("XMLInjector.Inject: Entity declaration?");
          }

        } // end match case

      } // end while

      ret
    })

    val nodesRDDPersisted = nodesRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // extract edges from nodes rdd
    val edgesDF = nodesRDDPersisted.flatMap((el: (Int, String, Int, String)) => {
      if (el._3 != Int.MinValue)
        List((el._3, el._1, el._4))
      else
        List()
    }: List[(Int, Int, String)]).toDF("src", "dst", "key")

    val rootNodeRDD = spark.sparkContext.parallelize(List(new NodeEntryPre(-1, "root", Int.MinValue, "")))

    // convert nodes rdd to a DataFrame with proper column names, drop edge info
    // we mark the resulting RDD as persistent on memory and disk, so that the
    // extraction result gets cached properly

    val nodesDF = (rootNodeRDD ++ nodesRDD).toDF("id", "value", "parent", "pkey").
      drop("parent", "pkey")

    // return the resulting graphframe
    GraphFrame(nodesDF.repartition(100).persist(StorageLevel.MEMORY_AND_DISK),
               edgesDF.repartition(100).persist(StorageLevel.MEMORY_AND_DISK))


  }

}
