package transformer
import interfaces.Transformer
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import utility.MTUtils

// -------------- transformation of the Class10-5-(1 to 6) Models ------------

class Class2Relational extends interfaces.Transformer {
  override def transform(spark: SparkSession, gf: GraphFrame): GraphFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    spark.conf.set("spark.sql.crossJoin.enabled", true)

    val startTimeMillis = System.currentTimeMillis()
    val relatTypeQtde = 5
    val typeId = "Integer"
    val vertices = gf.vertices
    val edges = gf.edges

    //----------------DataType filter-----------------------
    // link (id and dst) to get the the type of Data types of the model
    val lnkDataType = vertices.filter($"value"==="classmm:DataType")
      .select($"id").join(edges, $"id" === $"src")
      .select($"id".alias("idDtType"), $"dst")

    // --------------------Package Filters ---------------------
    //package filter: select the src and dst of all Packages for indicating their names
    val packages = vertices.filter($"value" === "classmm:Package")
    val srcDstPackages = packages.select($"id").join(edges
      .filter($"key" === "name"), $"id" === $"src")
      .select($"src",$"dst")

    //---------------Class Filter ---------------------------
      // Select the persistent classes from the GrapfFrames
      val persistClass = edges.filter($"key"==="isAbstract").select($"src",$"dst")
        .join(vertices).filter($"dst" === $"id" && $"value" === "false")
        .select($"src".alias("srcp"))

          // Select the links among class, package id, class type  and class super from GF Edges
      val persistClasEdges = persistClass.select($"srcp")
        .join(edges,$"srcp" === $"src")
        .select($"src", $"dst", $"key")

      val persistClasSrc = persistClass.select($"srcp")
        .join(edges, $"srcp" === $"dst")
        .select($"src", $"dst")

      val persistClassName = persistClasEdges.filter($"key" === "name")
        .select($"src", $"dst").join(vertices,$"dst" === $"id")
        .select($"src".alias("srcn"), $"value")

       // ------------------Common filter Attributes-------------------
        // Select the attributes from GF Vertices to the persistent classes
      val attVertices = vertices.filter($"value" === "att")
      val persClasAtt = persistClasEdges.select( $"src", $"dst", $"key")
        .join(attVertices,$"dst" === $"id")
        .select($"src".alias("origem"),$"id")

        // Select the attributes and their properties from persistent Classes
      val attEdgeProp = persClasAtt.join(edges,$"id" === $"src")
          .select($"origem", $"src", $"dst", $"key")

      val attType = attEdgeProp.filter($"dst" < relatTypeQtde )
        .select($"origem",$"src",$"dst".alias("dstAttType")).filter($"key"=== "name" || $"key" === "type")
      val attTypeid =  lnkDataType.join(attType,$"idDtType"===$"dstAttType")
        .select($"origem", $"src", $"dstAttType",$"dst")

      val attName = attEdgeProp.filter($"key" === "name")
        .select($"origem".alias("srccn"), $"src".alias("srcn"),$"dst".alias("dstAtt"))
        .join(vertices, $"dstAtt" === $"id")
        .select($"srccn",$"srcn", $"value".alias("name"))

      val attNameType = attName.join(attType, $"srccn"=== $"origem")
        .select($"srccn", $"src", $"name", $"dstAttType").distinct()

      val attTypeClas = attEdgeProp.filter($"dst" > relatTypeQtde)
      val attTypeClasName = attTypeClas.filter($"key" === "name")
        .select($"origem",$"src",$"dst".alias("dstClasType"))
        .join(vertices, $"id" === $"dstClasType")
        .select($"origem", $"src", $"value".alias("name"))

     // ------------------Simple Attribute Filters ..................................
      val simpleAtt = attEdgeProp.filter($"key" === "multivalued")
        .select($"origem", $"src", $"dst".alias("dstAtt"))
        .join(vertices, $"id" === $"dstAtt" && $"value" === "false")
        .select($"origem", $"src", $"id")

     // ---------------------------Multivaled Attribute Filters---------------------------
     // Select the attribute multivalued from persistent Classes
     val multivAtt = attEdgeProp.filter($"key" === "multivalued")
       .select($"origem", $"src", $"dst".alias("dstAtt"))
       .join(vertices, $"id" === $"dstAtt" && $"value" === "true")
       .select($"origem", $"src", $"id")

     val multClassName = multivAtt.join(persistClassName, $"origem" === $"srcn")
       .select($"origem", $"src".alias("srca"), $"value")

     val multClassAttName = multClassName.join(attNameType, $"srca" === $"src")
       .select($"origem",  $"value", $"name".alias("aname"), $"dstAtttype")

             //Select the Id of src and dst classes from multivalued attributes
     val srcDstClass = multClassName.as('one).select($"origem", $"srca")
       .join(attTypeClas.as('two).filter($"key" === "type"), $"one.srcc" === $"two.srcc")
       .select($"one.srcc", $"srca", $"dst".alias("dstc"))

     //select multivalued attribute ids from edges to form relationship between tables
     val srcDstAtt = srcDstClass.join(edges, $"dstc" === $"src")
       .select($"origem",  $"dst".alias("dsta"))
     val dstAttId = srcDstAtt.join(attVertices, $"dsta" === $"id")
       .select($"origem", $"id")

     val dstAttNameId = dstAttId.join(edges.filter($"key" === "name"), $"id" === $"src")
       .select($"origem".alias("srcd"), $"dst")

     val srcAttNameId = srcDstClass.join(edges, $"srca" === $"src")
       .filter($"key" === "name").select($"origem", $"dst".alias("dsts"))

     val dstAttName = dstAttNameId.join(vertices, $"dst" === $"id")
       .select($"srcd", $"value".alias("named"))
     val srcAttName = srcAttNameId.join(vertices, $"dsts" === $"id")
       .select($"origem", $"value".alias("name"))


     // ---------------- Transformation Rules -----------------------------
     // ClassDataTypes2RelationalDataTypes
     val relationalDtTypeDF = lnkDataType.join(vertices, $"id" === $"dst")
       .select($"id", $"idDtType", $"value")

     // Package2Schema Rule
     val schemaDF = srcDstPackages.join(vertices,$"dst" === $"id")
       .select($"src", $"value".alias("schema"))

     //Class2Table Rule
     val lnkSchClas = schemaDF.select($"src".alias("srcSch")).join(persistClasSrc)
       .filter($"srcSch" === $"src").select($"src", $"dst")
     val tableDF = lnkSchClas.select($"src", $"dst").join(persistClassName,
      $"origem" === $"dst").select($"src", $"value".alias("name"), $"dst")
     val objColumnDF = tableDF.select($"dst", lit("objectId")
       .alias("name"),lit(typeId).alias("type"))

     //SingleValuedDataTypeAttribute2Column Rule
     val dataTypeColumnDF = simpleAtt.join(attNameType, $"srccn" === $"origem")
       .select($"origem", $"name", $"dstAttType")

    //ClassAttribute2Column Rule
    val clasAttColumnDF = attTypeClasName
      .select($"origem", concat($"name", lit("Id"))
      .alias("name"), lit(typeId).alias("type"))

    //MultiValuedDataTypeAttribute2Column Rule
    val multDataTypeTable = multClassAttName
      .select($"origem", concat($"value", lit("_"), $"aname").alias("name"))
    val multValdDataTypeColumnId = multClassAttName
      .select($"origem", concat($"aname", lit("Id")).alias("name"),lit(typeId).alias("type"))
    val multValdDataTypeColumnDF = multClassAttName.select($"origem", $"aname"
      .alias("name"), $"dstAtttype")

    //MultiValuedClassAttribute2Column Rule
    val assAttName = srcAttName.join(dstAttName,$"origem" === $"srcd")
    val assTable =  assAttName.select($"origem", concat($"name",
      lit("_"), $"named").alias("name"))
    val multValdColumnId = assAttName.select($"origem", concat($"name",
      lit("Id")).alias("name"), lit(typeId).alias("type"))
    val multValdCollumnFkDF = assAttName.select($"origem", concat($"named",
      lit("Id")).alias("name"), lit(typeId).alias("type")).distinct()


    //------- Union and join output rules -----------------------

    val allTableDF = objColumnDF.union(tableDF).coalesce(1)
    val allCollumDF = multValdCollumnFkDF.union(multValdDataTypeColumnDF)
      .union(clasAttColumnDF).union(dataTypeColumnDF)
    val relationalAll = schemaDF.join(allTableDF, $"id" === $"origem")
      .join(allCollumDF,$"origem" === $"allTableDF.origem")

    val relationalDF = relationalAll.union(relationalDtTypeDF).coalesce(1)

    relationalDF.write
      .option("rootTag", "Schema")
      .xml("relational.xml")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.000
    println(durationSeconds)
    gf
  }
}

