/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.wish.spark.clustering

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.io.IOUtils;
import scala.util.control.Breaks._

object Coclustering {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spectral Coclustering Algorithm")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","24")               // Now it's 24 Mb of buffer by default instead of 0.064 Mb

    val sc = new SparkContext(conf)
    
   // val file = "hdfs://localhost:8070/testEdgeListFile2")
   //val file = "hdfs://192.168.0.52:8070/testEdgeListFile2"
    //var file = "/home/fubao/workDir/CiscoWish/QueryGraph/mazerunner/sparkMaven/testEdgeListFile2"
    inputDataToClusterOnePair(sc, args)
  }
  
  
 def inputDataToClusterOnePair(sc: SparkContext, args: Array[String]) = {
   
    var numClusters = 5
    if (args.length >= 1)
      numClusters = args(0).toInt
    
    println ("numCluster : ", numClusters)
    
    var testLimitNodes = 100000
    if (args.length >= 2)
      testLimitNodes = args(1).toInt

    val numericalInputColumnPath = "data/inputColumns/numericals/InputPairsNumerical.csv"
   // val numericalGeneColumnPairPath = "data/inputColumns/numericals/allNumericalFinalResultManuallyVerified.csv"
    val numericalInputColumnPairsRdd = sc.textFile(numericalInputColumnPath).map(line => line.split(','))
   // val columnNameGeneratePairsRdd = sc.textFile(numericalGeneColumnPairPath).map(line => line.split(','))
    
    //combine the two pairs RDD and transfer to array in local driver, it's ok with small records pairs
    val columnNameAllPairsArray = numericalInputColumnPairsRdd.toArray

    var index = 0
    var noPairs = 1
    if (args.length >= 3)
      noPairs = args(2).toInt
    //val biparColumnA = columnNameAllPairsArray(2)      // "family"               //change to args according to table
    //val biparColumnB = columnNameAllPairsArray(3)       //"problem_code"         //change to args according to table
    
    for ( columnPair <- columnNameAllPairsArray ) {
       // println("columnPair ", columnPair)
        if (index == noPairs){                 //test which line of pair
            // println("columnPair222 ", columnPair(0),columnPair(1))
             getBipartitePairs(sc, testLimitNodes, columnPair, numClusters, noPairs)
        }
        index += 1
      }    
 }
 
 
 def getBipartitePairs(sc: SparkContext, testLimitNodes: Int, correColumnPair: Array[String], numClusters: Int, pairNo: Int) = {
   val dataDir = "data/service_request_staging_DB"
   val tblA = correColumnPair(0).split("\\.")(0).toUpperCase    
   val correColumnA = correColumnPair(0).split("\\.")(1).toLowerCase     
   val tblB = correColumnPair(1).split("\\.")(0).toUpperCase    
   val correColumnB = correColumnPair(1).split("\\.")(1).toLowerCase
   //get all the fields
   val fileFieldsA = dataDir + "/" +tblA +"/desc.txt"
   val fileFieldsB = dataDir + "/" +tblB +"/desc.txt"
   val fieldsARdd = sc.textFile(fileFieldsA).map(line => line.split('\t'))
   val fieldsBRdd = sc.textFile(fileFieldsB).map(line => line.split('\t'))
    val testfieldAFilterRdd = fieldsARdd.map{
        case(fieldAttrs) => fieldAttrs(1).toLowerCase.stripMargin
      }
 //  testfieldAFilterRdd.take(20).foreach(println)

   //filter pair first, use non-numericla only first
   val fieldAFilterRdd = fieldsARdd.filter{
   case(fieldAttrs) => (fieldAttrs(1).toLowerCase.replaceAll("\\s", "").indexOf("string") != -1)   
 }.map(fieldAttrs => fieldAttrs(0).toLowerCase.stripMargin)
    fieldAFilterRdd.take(10).foreach(println)

    val fieldBFilterRdd = fieldsBRdd.filter{
   case(fieldAttrs) => (fieldAttrs(1).toLowerCase.replaceAll("\\s", "").indexOf("string") != -1)   
 }.map(fieldAttrs => fieldAttrs(0).toLowerCase.stripMargin)
    fieldBFilterRdd.take(10).foreach(println)

   val bipartFieldPairsRdd = fieldAFilterRdd.cartesian(fieldBFilterRdd)
   println("ccccccc92  ", bipartFieldPairsRdd.count)
  // bipartFieldPairsRdd.take(10).foreach(println)
   
   val bipartFieldPairsArray = bipartFieldPairsRdd.toArray
   var testLoop = 0
   for (columnPair <- bipartFieldPairsArray)
    {
     // if (testLoop > 1)
      //  {
       //   break
      //  }
     // testLoop += 1
      val biparColumnA = columnPair._1.toLowerCase.stripMargin.replaceAll("\\s", "");
      val biparColumnB = columnPair._2.toLowerCase.stripMargin.replaceAll("\\s", "");
    //  copyToLocal(tblA, tblB, biparColumnA, biparColumnB, pairNo)
      executCluster(sc, testLimitNodes, tblA, tblB, correColumnA, correColumnB, biparColumnA, biparColumnB, numClusters, pairNo)
    }
}

 def copyToLocal(tblA:String, tblB: String, biparColumnA: String, biparColumnB: String, pairNo: Int) = {
  val hadoopconf = new Configuration();
  val hdfs = FileSystem.get(new java.net.URI("hdfs://master:9000"),hadoopconf);
  //Create output stream to HDFS file
  val existFlag = hdfs.exists(new Path("hdfs://master:9000/user/jiangtao/output"))
  if (existFlag){
    val srcPath = new Path("hdfs://master:9000/user/jiangtao/output")
    //Create input stream from local file
  val outpath = "file:/home/jiangtao/mazerunner/sparkMaven/outputAllPairs/" + pairNo.toString +"-" + tblA + "_" + biparColumnA + "__" +  tblB + "_" + biparColumnB
  val destPath = new Path(outpath)
  println ("eeeeeeeeeeeeeeeee " + srcPath, destPath)

  hdfs.copyToLocalFile(srcPath, destPath)
}
  
}
                                           
  // clustering algorithm
 def executCluster(sc: SparkContext, testLimitNodes: Int, tblA:String, tblB: String, correColumnA: String, correColumnB: String, biparColumnA: String, biparColumnB: String, numClusters :Int, pairNo: Int) = {
   
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new java.net.URI("hdfs://master:9000"), hadoopConf)
   // val outputClusterResultFile = "output"+"/" +pairNo.toString +"-" + tblA + "_" + biparColumnA + "__" +  tblB + "_" + biparColumnB+ "/" +correColumnA + "-" + correColumnB
    val outputPreprocessOrigFile = "output"+"/" + pairNo.toString +"-" + tblA + "_" + biparColumnA + "__" +  tblB + "_" + biparColumnB+ "/" +"outputPreprocessData"
    try { hdfs.delete(new Path(outputPreprocessOrigFile), true) } catch { case _ : Throwable => { } }
  //  try { hdfs.delete(new org.apache.hadoop.fs.Path(outputPreprocessOrigFile), true) } catch { case _ : Throwable => { } }
    
   val existFlag = hdfs.exists(new Path(outputPreprocessOrigFile))
   if (!existFlag)
   {
     val dataDir = "data/service_request_staging_DB"
     println("ddddddd ", correColumnA, correColumnB, biparColumnA, biparColumnB)
   //println("tblA  :", tblA(0), columnA.toString)
     val fileA = dataDir + "/" +tblA +"/000000_combine.tsv"
     val fileB = dataDir + "/" +tblB +"/000000_combine.tsv"
     val edglistCombine = readTableDataCombineNodes.readFileToEdgeListCombinedRdd(testLimitNodes, fileA, fileB, correColumnA, correColumnB, 
                        biparColumnA, biparColumnB, sc, outputPreprocessOrigFile: String)
  // val coClusterServiceReqObj = new CoclusteringAlg2()
  // coClusterServiceReqObj.kMeansCluster(edglistCombine, sc, numClusters)   
   } 
 }   
}

