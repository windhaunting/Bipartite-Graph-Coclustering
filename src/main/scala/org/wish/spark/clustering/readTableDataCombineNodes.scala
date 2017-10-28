/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.wish.spark.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow,IndexedRowMatrix}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType, IntegerType, LongType, DoubleType};
import org.apache.spark.sql.Row

object readTableDataCombineNodes {

  
   //filter null value and non numerical value
   def filterNumericalNullValues(inStr: String) ={
     import util.control.Exception._
    catching(classOf[NumberFormatException]) opt inStr.toLong
    
   }
   
   //filter null value of string
   def filterStringNullValues(inStr: String) ={
     val inStrPre = inStr.toLowerCase.stripMargin
     if ((inStrPre == "\\N") || (inStrPre == "\\n") || (inStrPre == "\n") || (inStrPre == " ") || (inStrPre == "n/a") || (inStrPre == "na")  || (inStrPre == "none"))
        None
   }
  
   //get column fields index
   def getcolumfieldIndex(column: String, schema: StructType) :Int = {
     
     val strfields = schema.fields
      
     for(i <- 0 until strfields.length)
     {
       //println("strfields(i).name: ", strfields(i).name, column)
       if(strfields(i).name == column)
        { 
         return i
        }
     }
     return -1
   }
     
   //biparColumnA and biparColumnB should be connected to a new bipartite graph
   //CorrecolumnA and CorrecolumnB is the original correlated columns
   //family + problem code as new bipartite graph
   def readFileToEdgeListCombinedRdd(testLimitNodes: Int, pathfileA :String,
               pathfileB :String, correColumnA: String, correColumnB: String, 
               biparColumnA: String, biparColumnB: String, sc: SparkContext, outputPreprocessOrigFile: String) :(RDD[String],RDD[String],RDD[(String, (String, Double))],RDD[(String, Iterable[(String, Long)])],RDD[(String, Iterable[(String, Long)])])=  {
      
   // val fileA = sc.textFile(pathfileA)
   // fileA.take(5)
    
     val sqlContext = new SQLContext(sc)
     val dfA = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .option("delimiter", "\t")
    .option("mode", "PERMISSIVE")
    .option("parserLib","univocity")
    .option("quote", " ")
    .load(pathfileA)
    //.limit(testLimitNodes)
    
    // dfA.take(20).foreach(println)
    //hash partitions, without preserving the same order as the orginal input table
    val tableAIndRdd =  dfA.rdd.zipWithIndex.map{
     case(v, k)=>(k,v)
   }                                  //get index of all rows for later use of coclustering
    // import sqlContext.implicits._
     //get the column A records RDD
    // dfA.take(10).foreach(println)
     print(" tableA partition size ", tableAIndRdd.partitions.size)
     
     val schemaA = dfA.schema
     val indexFieldbiparA = getcolumfieldIndex(biparColumnA, schemaA)
     assert(indexFieldbiparA != -1)              //make sure it's found    
     val colIndBiparARdd = tableAIndRdd.map{
       case(i, f)=> (i, f(indexFieldbiparA).toString)                 //I,F
     }
      
   // println("aaaaaa 54:  ", colIndBiparARdd.partitions.size)
   // colIndBiparARdd.take(10).foreach(println)
     val indexFieldcorreA = getcolumfieldIndex(correColumnA, schemaA)
     assert(indexFieldcorreA != -1)   
     val colIndCorreARdd = tableAIndRdd.map{
       case(i, k)=> (i, k(indexFieldcorreA).toString)                       //I,K
     }
     
    // println("aaaaaa 62  ")
    //colIndCorreARdd.take(10).foreach(println)

    //after join to shuffle, need to sortByKey(true)?
    //Here correColumnA is only for numerical, modify (not to filter > 0)to fit if it's non-numerical later
    val leftColumnCombineARdd = colIndBiparARdd.join(colIndCorreARdd).filter{             //I,(F,K)
       case(i, (f,k))=>(filterNumericalNullValues(k.toString) != None)                //filter numerical          
     }.filter{
       case(i, (f,k))=>(k.toString.toLong >=0)       //filter numerical bigger than 0
     }.filter{
        case(i,(f,k))=>(filterStringNullValues(f.toString) != None)  //filter non-numerical
     }         
     //I, F, K
   // println("aaaaaa 65   ")
  //  leftColumnCombineARdd.take(5).foreach(println)      
    
     //save to local file or hdfs 
    val outputDataA = outputPreprocessOrigFile + "/tableAColumns"
   //  tableAIndRdd.saveAsTextFile(outputDataA)
    val firstStrA = StructField("Index No", LongType, true)
    var schemaArrayA = new Array[StructField](schemaA.fields.length+1)
    schemaArrayA(0) = firstStrA
    for(i <- 0 until schemaA.fields.length)
        schemaArrayA(i+1) = schemaA.fields(i)     
    val newschemaArrayA = StructType(schemaArrayA) 
    import sqlContext.implicits._
    val tableAIndRddOutput = tableAIndRdd.map{
      case(k, v)=>Row.fromSeq(k +: v.toSeq)
    }
    val tableAIndRddOutputDF = sqlContext.createDataFrame(tableAIndRddOutput, newschemaArrayA)
   //  tableAIndRddOutputDF.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save(outputDataA)

      val numTasks = 6
     //get bipartite column as key
     val bipartCombineRddStoreA = leftColumnCombineARdd.map{
       case(i, (f,k))=>(f, (k, i))                           //F, (K, I)
     }.groupByKey(numTasks)          //number of tasks         //F, iter((K,I))
                                          
                                          
     val correCombineRddA = leftColumnCombineARdd.map{
       case(i, (f,k))=>(k,(i,f))                      //K,(I,F)
     }   
     
//******************************************************************************
     
     //table B
     val dfB = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .option("delimiter", "\t")
    .option("mode", "PERMISSIVE")
    .option("parserLib","univocity")
    .option("quote", " ")
    .load(pathfileB).limit(testLimitNodes)
    
    // dfB.take(5).foreach(println)
    //filter
     val tableBIndRdd =  dfB.rdd.zipWithIndex.map{
     case(v, k)=>(k,v)
   }    
   //  val numPartitions  = 4               // hdfs default size is 125M , but not partitioned here?
   //  tableBIndRdd.repartition(numPartitions)
   // println("aaaaaa 130   ")
  //  tableBIndRdd.take(5).foreach(println)
     print(" tableB partition size ", tableBIndRdd.partitions.size)

    val schemaB = dfB.schema
     val indexFieldbiparB = getcolumfieldIndex(biparColumnB, schemaB)
     assert(indexFieldbiparB != -1)
     
     val colIndBiparBRdd = tableBIndRdd.map{
       case(i, p)=> (i, p(indexFieldbiparB).toString)
     }
                                                           //I,P
  //  println("aaaaaa 142      ")
   // colIndBiparBRdd.take(5).foreach(println)

    val indexFieldcorreB = getcolumfieldIndex(correColumnB, schemaB)
    assert(indexFieldcorreB != -1)
     
     val colIndCorreBRdd = tableBIndRdd.map{
       case(i, k)=> (i, k(indexFieldcorreB).toString)
     }                                                   //I, K
     
   // println("aaaaaa 159      ")
   // colIndCorreBRdd.take(5).foreach(println)

    //after join to shuffle, need to sortByKey(true)?    
    val rightColumnCombineBRdd = colIndBiparBRdd.join(colIndCorreBRdd).filter{
       case(i,(p,k))=>(filterNumericalNullValues(k.toString) != None)                               // ++mm
     }.filter{
       case(i,(p,k))=>(k.toString.toLong >0)             //I,P,K, filter the correlated column negative values and else?
     }.filter{
        case(i,(p,k))=>(filterStringNullValues(p.toString) != None)    //filter null value
     }                                                
  // println("aaaaaa 165      ")
  //  rightColumnCombineBRdd.take(5).foreach(println)
    
     //save to hdfs
    val outputDataB = outputPreprocessOrigFile + "/tableBColumns"
   //  tableBIndRdd.saveAsTextFile(outputDataB) 
    val firstStrB = StructField("Index No", LongType, true)
    var schemaArrayB = new Array[StructField](schemaB.fields.length+1)
    schemaArrayB(0) = firstStrB
    for(i <- 0 until schemaB.fields.length)
        schemaArrayB(i+1) = schemaB.fields(i)     
    val newschemaArrayB = StructType(schemaArrayB) 
    import sqlContext.implicits._
    val tableBIndRddOutput = tableBIndRdd.map{
      case(k, v)=>Row.fromSeq(k +: v.toSeq)
    }
      val tableBIndRddOutputDF = sqlContext.createDataFrame(tableBIndRddOutput, newschemaArrayB)
 //    tableBIndRddOutputDF.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save(outputDataB)

    val bipartCombineRddStoreB = rightColumnCombineBRdd.map{
       case(i, (p,k))=>(p, (k, i))                     //P, (K,I)
     }.groupByKey(numTasks)        //number of tasks     //P， Iter((K,I))                    
     
     
     val correCombineRddB = rightColumnCombineBRdd.map{
       case(i, (p,k))=>(k,(i, p))                      //K,(I,P)
         
     }  
    //extract the corresponding of biparte columns,  F,P
    val edgeWeight = 1.0
    val bipartEdgeRdd = correCombineRddA.join(correCombineRddB).map{                 //K, ((I,F),(I,P))
      case(k, (iff, ip))=> ((iff._2, ip._2), edgeWeight)                  //(F,P), Weight
    }.reduceByKey(_ + _).map{
      case((f,p), w)=>(f,(p, w))                   //get new weight _+_; reduceByKey((v1, v2) => v1).   //f,p,w
    }
    
   // assert(bipartEdgeRdd.count != 0)  
    if (bipartEdgeRdd.count == 0)
      {
        return null
      }
    //edgeListRddIndex.saveAsTextFile(edgeListFile)    //to RDD column A, index_id
    println("aaaaaa 222      ")
  //  bipartEdgeRdd.take(10).foreach(println)
     //write edge list file
     val edgeListFile = outputPreprocessOrigFile + "/edgelistFiletsv"
     val edgeListRddIndex = bipartEdgeRdd.map{
       case(f,(p, w))=>(w,(f,p))
     }.sortByKey(false)
     val schemaString = "first_field Second_field Weight"
     val schemaStrArr = schemaString.split(" ")
     val schemaEdge =  StructType(
     StructField(schemaStrArr(0), StringType, true) ::
     StructField(schemaStrArr(1), StringType, true) ::
     StructField(schemaStrArr(2), DoubleType, true) :: Nil)

    val edgeListRDD = edgeListRddIndex.map(p => Row(p._2._1,p._2._2, p._1))
    val edgeListDF = sqlContext.createDataFrame(edgeListRDD, schemaEdge)
    edgeListDF.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save(edgeListFile)
      

    //get row and column of adjacency matrix
        //to RDD column A, index_id
    val colBiparAFinalRdd = bipartEdgeRdd.map{
      case(f,(p, w)) =>f
    }.distinct                                       //f
   
    val colBiparBFinalRdd = bipartEdgeRdd.map{
      case(f,(p, w)) =>p
    }.distinct                                      //p
    
    println("dddad 237 rowNumber, colBFinalRdd, edgeListRdd number: ", colBiparAFinalRdd.count, colBiparBFinalRdd.count, bipartEdgeRdd.count)
    
    Tuple5(colBiparAFinalRdd, colBiparBFinalRdd, bipartEdgeRdd, bipartCombineRddStoreA, bipartCombineRddStoreB)
  
  }
  
  //transfer edglist to indexedrowMatrix
  def egeListRddToIndexRowMatrix(colAFinalRdd: RDD[String], colBFinalRdd: RDD[String], edgeListRdd: RDD[(String, (String, Double))]) = {
    
    val rowIndexReverseRdd = colAFinalRdd.zipWithIndex      //F,I    ; I is index
    
    val rowIndexRdd = rowIndexReverseRdd.map{
       case (f,i) =>(i,f)}.sortByKey(true)                  //I,F
   //  println("dddad  127")
   //  rowIndexRdd.take(5).foreach(println)   
  
    val colIndexReverseRdd = colBFinalRdd.zipWithIndex        //P,J
    
     val colIndexRdd = colIndexReverseRdd.map{
       case (p,j) =>(j,p)}.sortByKey(true)            //J,P
  //   println("dddad  132")
  //   colIndexRdd.take(5).foreach(println) 
     val colNum = colIndexRdd.count
     
    //Step 1
    val colEdgeRdd = edgeListRdd.map{
      case(f, (p, w))=>(p, (f, w))                         //P,(F,W)
    }
    //step 2
    val rowColCombineRdd = colIndexReverseRdd.join(colEdgeRdd).map{                //(P, (J,(F,W)))
      case(p,(j,(f, w))) =>(f, (j,w))                                 //(F,(J,W))
    }
    
    //step3
    val numTasks = 5
    val rowsJoinsIndexRdd = rowIndexReverseRdd.join(rowColCombineRdd).map{            //(F,(I,(J,W)))
      case(f, (i,(j,w)))=>(i,(j,w))
    }.groupByKey(numTasks).map{
      case(i, iterJW)=>(i, iterJW.toSeq.map{
          case(j, w)=>(j.toInt, w)})
    }.sortByKey(true).map{
      case(i, seqJW) => IndexedRow(i, Vectors.sparse(colNum.toInt, seqJW))  // Vectors.sparse(3, Seq((0, 1.0), (2, 3.0))) sparseIndexedRow(k,v.toVector)
    }
    
  //  println("dddad  282")
  //  rowsJoinsIndexRdd.take(15).foreach(println)

    val IndexRowMatA = new IndexedRowMatrix(rowsJoinsIndexRdd)
    
    Tuple3(IndexRowMatA, rowIndexRdd, colIndexRdd)
  }
   
}
