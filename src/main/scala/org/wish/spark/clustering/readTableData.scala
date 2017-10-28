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

object readTableData {

  /*
   def testToAdjMatrix(sc :SparkContext) = {
   
case class Person(name: String, age: Int)
  // val sqlContext = new SQLContext(sc)
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   import sqlContext.implicits._
  val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF //("single", "double")

   val rdd = sc.parallelize(Seq(
  ("a", "developer"), ("b", "tester"), ("b", "developer"),
  ("c","developer"), ("c", "architect")))

   val df = rdd.toDF("row", "col")
   
   }
   */
  
   def readFileToEdgeListRdd(testLimitNodes: Int, pathfileA :String, pathfileB :String, columnA: String, columnB: String, sc: SparkContext) = {
      
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
    .load(pathfileA).limit(testLimitNodes)
    
    // dfA.take(20).foreach(println)
    // dfA.printSchema()
     
    //filter the negative value and non string value
    
    val newdfA =  dfA.filter(dfA(columnA) > 0 )
   // newdfA.take(5).foreach(println)
    // println(prodKeyColAProd.toString)
     val tableAIndRdd =  newdfA.rdd.zipWithIndex
     println("dddddd tableA count ", tableAIndRdd.count)
    // import sqlContext.implicits._
     //get the column A records RDD
     val colIndARdd  = newdfA.select(columnA).rdd.zipWithIndex.map{
     case(v, k)=>(v.toSeq(0).toString, k.toString +"_" +v.toSeq(0).toString)
    }  
    
     //tableAIndRdd.take(5).foreach(println)
     //println("aaaaaa 67      ")
     colIndARdd.take(5).foreach(println)
     //save to local file or hdfs 
     val outputDataA = "outputModifiedData/tableAColumns"
     tableAIndRdd.saveAsTextFile(outputDataA)
    
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
    // dfB.printSchema()
    //filter
     val newdfB =  dfB.filter(dfB(columnB) > 0)
     val tableBIndRdd =  newdfB.rdd.zipWithIndex
     println("dddddd tableB count ", tableBIndRdd.count)

     //get the column B records RDD
     val colIndBRdd  = newdfB.select(columnB).rdd.zipWithIndex.map{
     case(v, k)=>(v.toSeq(0).toString, k.toString +"_" +v.toSeq(0).toString)
    }
    
     //save to hdfs 
     val outputDataB = "outputModifiedData/tableBColumns"
     tableBIndRdd.saveAsTextFile(outputDataB)
    
    colIndBRdd.take(5).foreach(println)
    //groupby
   // val colIndBGrpLstRdd = colIndBRdd.groupByKey()          // val, (index, val)...., 29, (0,29)，（1,29)， （2, 29) 
  //  colIndBGrpLstRdd.take(5).foreach(println)       
    
    //form adjacency list
   // colIndBGrpLstRdd join
   //filter the result to test the memory performance
   
    val edgeWeight = 1.0
    val edgeListRdd = colIndARdd.join(colIndBRdd).map{
      case(k, v) =>(v._1, (v._2, edgeWeight))
    }
   // edgeListRdd.take(5).foreach(println)
    
    /*
    val filterNumTest = 400000
    val edgeListRdd = edgeListRddOriginal.zipWithIndex.map{
      case(v, k) => (k, v)
    }.filter {
    _ match {
        case (k,v) => k < filterNumTest
        case _ => false     //in case of invalid input
    }
}.map{
      case(k, v) =>v
    }
*/
    //to RDD column A, index_id
    val colAFinalRdd = edgeListRdd.map{
      case(v1, (v2, e)) =>v1
    }.distinct
   
    val colBFinalRdd = edgeListRdd.map{
      case(v1, (v2, e)) =>v2
    }.distinct
   
    //colAFinalRdd.take(5).foreach(println)
    println("dddad 113 rowNumber, colBFinalRdd, edgeListRdd number: ", colAFinalRdd.count, colBFinalRdd.count, edgeListRdd.count)
    //colBFinalRdd.take(5).foreach(println)
    
    Tuple3(colAFinalRdd, colBFinalRdd, edgeListRdd)
  }
  
  //transfer edglist to indexedrow
  def EdgeListRddToIndexRowMatrix(colAFinalRdd: RDD[String], colBFinalRdd: RDD[String], edgeListRdd: RDD[(String, (String, Double))]) = {
   
     val rowIndexRdd = colAFinalRdd.zipWithIndex.sortByKey(true).map{
       case (k,v) =>(v,k)}.sortByKey(true)
     println("dddad  127")
     rowIndexRdd.take(5).foreach(println)          

     val colIndexRdd = colBFinalRdd.zipWithIndex.map{
       case (k,v) =>(v,k)}.sortByKey(true)
     println("dddad  132")
     colIndexRdd.take(5).foreach(println) 

     val colNum = colIndexRdd.count
     val colMap = colIndexRdd.map{
       case (k,v)=>(v,k)}.toArray.toMap                  //not good to scale?????
     
  //  val taskNum = 4              //how many to be set?
    val rowsJoinsRdd = rowIndexRdd.map{
       case (k,v)=>(v,k)}.join(edgeListRdd.groupByKey())
     .map{
      case (k, vs) => (vs._1, vs._2.map{
      case (k2, vs2) => (colMap(k2).toInt, vs2.toDouble)             //a better way to optimize here?????????
      })
    }.sortByKey(true).map{
      case(k, v) => IndexedRow(k, Vectors.sparse(colNum.toInt, v.toSeq))  // Vectors.sparse(3, Seq((0, 1.0), (2, 3.0))) sparseIndexedRow(k,v.toVector)
    }
    
    println("dddad  152")
    rowsJoinsRdd.take(5).foreach(println)

    val IndexRowMatA = new IndexedRowMatrix(rowsJoinsRdd)
    
    Tuple3(IndexRowMatA, rowIndexRdd, colIndexRdd)
    
  }
}
