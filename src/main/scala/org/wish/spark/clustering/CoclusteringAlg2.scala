/*
 *05-12-2016
 *by fubao
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.wish.spark.clustering

import org.apache.spark.mllib.linalg.{Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, MatrixEntry}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType, IntegerType, LongType};


class CoclusteringAlg2 {


 def RowMatrixtoRDD(m: RowMatrix, sc: SparkContext): RDD[Vector] = {
 // val rMat = m.toRowMatrix()
 // val columns = RowMatrix.toArray.grouped(m.numRows)
  
  //val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
  //val vectors = rows.map(row => new DenseVector(row.toArray))
  // sc.parallelize(vectors)
  
  val rvec:RDD[Vector] = m.rows
  //sc.parallelize(rvec)
  rvec
}

  //matrix to rdd by column  
  def matrixToRDD(m: Matrix, sc: SparkContext): RDD[Vector] = {
   val columns = m.toArray.grouped(m.numRows)        //m is a matrix with column-major right singular vector
  val rows = columns.toSeq.transpose // Skip this if a column-major RDD is needed
  val vectors = rows.map(row => new DenseVector(row.toArray))                 //to sparse vector?
   
   sc.parallelize(vectors)
}

   //matrix to rdd by rows
  def matrixToRddVector(mat: IndexedRowMatrix) = {
      mat.rows.map(
       ind => Tuple2(ind.index, ind.vector)).sortByKey(true)
  }

  
  //create vector with same value
  /*
   * 1 1 1 1...
   */
  def createVec(ele :Double, nums :Int) :Vector =
    {
      val arr = Array.fill[Double](nums)(ele)
      val dv: Vector = Vectors.dense(arr)
      dv
    }
    
  //get array[vectors] 
  /*
   * 1 1 1 1
   * 2 2 2 2
   * 
   */
  def createListVec(rowNums: Long, Vecs :Vector, colNums: Int) =
    {
      var listVecs = List[Vector]()
      
      for (ele <- Vecs.toArray)
        {
            val dv = createVec(ele, colNums)
            listVecs :+= dv
        }
     // val arrVec = Array.fill[Vector](rowNums)(dv)
      listVecs
    }

 

  //get the iagnal matrix D of laplacian transform
  def normalizeMatrixA(indRowMat :IndexedRowMatrix) =    //:IndexedRowMatrix= 
    {      
     //get column diagonal matrix's diagonal values, 
    val colSums = indRowMat.toCoordinateMatrix.transpose.entries.map{case MatrixEntry(row, _, value) => (row, value)}.reduceByKey(_ + _).sortByKey(true)
   
    //colSums.take(5).foreach(println)
      
    val colD2vec :Vector = Vectors.dense(colSums.map(e => 1.0/(Math.sqrt(e._2))).toArray)
      
     //println("wfb 194 ")    
    //normalized to An
    val matRddAn = indRowMat.rows.map{
      case IndexedRow(k, v) => 
      //get row vectorSums and scaling of 1/sqrt(vector) 
    def getSumSqrtInverVecScaling(vecA :SparseVector, vecB :SparseVector) :Vector =
    {
      val scaling = 1.0/ Math.sqrt(vecA.toArray.sum)
      Vectors.sparse(indRowMat.numCols.toInt, vecA.indices, vecB.values.map(_*scaling))           //vecA.toArray.length, int 2^32 signed value
     }
     
     (k, getSumSqrtInverVecScaling(v.toSparse, v.toSparse))
    }.map{
          case (k, v) =>
          val transformer = new ElementwiseProduct(colD2vec)
         IndexedRow(k, transformer.transform(v))
   }.persist(MEMORY_AND_DISK_SER)                         //cache?
    
    //println("wfb -- 250: ")
    //matRddAn.take(5).foreach(println)
    
    val MatrixAn :IndexedRowMatrix = new IndexedRowMatrix(matRddAn)
    new Tuple2(MatrixAn, colSums)
  }
  
  
  //svd Decomposition, discard the first n_discard singular value;  val mat: IndexedRowMatrix;
  def svdDecompose(mat :IndexedRowMatrix, r :Int, n_discard :Int, sc :SparkContext) = {
   mat.toRowMatrix.rows.cache()             //cache?
   val svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = mat.computeSVD(r, computeU = true)
   val u_org :IndexedRowMatrix = svd.U     // The U factor is a RowMatrix.
  // val s_org: Vector = svd.s                // The singular values are stored in a local dense vector.
   
   //filter the n_discard column
   //println("wfb 227: ")
   val rddU = matrixToRddVector(u_org).map{
     case (k,v) => IndexedRow(k, Vectors.dense(v.toArray.take(v.size -n_discard)))      //drop the first n_discard, change to sparse or not?
   }
   
  // val u = new IndexedRowMatrix(rddU)

   val v_org :Matrix = svd.V                // The V factor is a local dense matrix
   //filter the n_discard row
   val columnRdd = matrixToRDD(v_org, sc)
   //println("wfb 241: ", r)
  // columnRdd.take(5).foreach(println)
   
   val rddV = columnRdd.zipWithIndex.map{
     case (v,k) => IndexedRow(k, Vectors.dense(v.toArray.take(v.size -n_discard)))      //drop the first n_discard, change to sparse or not?
   }
  
 //  val v = new IndexedRowMatrix(rddV)
   (rddU,rddV)
}

  
  //get v2, v3, ..., vl+1 vectors
  def getVectorForCluster(indRowMat: IndexedRowMatrix, MatrixAn: IndexedRowMatrix, colSums: RDD[(Long, Double)], k: Int, n_discard: Int, sc: SparkContext) = {
      val r :Int = 1 + Math.ceil(common.log2(k)).toInt
      val (rddU, rddV) = svdDecompose(MatrixAn, r, n_discard, sc)
    //  println("198.....  ")
    //  rddU.take(5).foreach(println)
    //  rddV.take(5).foreach(println)

    //  val rowSums :RDD[(Long, Double)] = coo_matrix.entries.map{case MatrixEntry(row, _, value) => (row, value)}.reduceByKey(_ + _).sortByKey(true)
    val rowSums: RDD[(Long, Double)] = indRowMat.rows.map{
      case IndexedRow(i, values) => (i, values.toArray.sum)
    } //.sortByKey(true)
    
  //  rowSums.take(5).foreach(println)
      
    //
      val ArrayD1RDD = rowSums.map{
        case e =>(e._1,
      //  Array.fill[Double](coo_matrix.numCols.toInt)(1/(Math.sqrt(e._2)))) 
      1.0/(Math.sqrt(e._2)))
     }
     
      //ArrayD1RDD.take(5).foreach(println)
     //  println("285  -- ")
     //get the result of multipling D1 diagnaol to U,      very time-consuming, so we use vector rdd times vector
      val uRdd = rddU.map{
        case r => (r.index, IndexedRow(r.index,r.vector))
      }
      
     //  println("288  -- ")
      val groupedzMatUpRdd = ArrayD1RDD.join(uRdd)      // (k, (V, W))
                                             
      val zMatUpRdd = groupedzMatUpRdd.map{
        case (k, vw) => 
        //val IndexmultiRes = v._2
           
         def multiply(vec :Vector, scaling :Double) :Vector = {
          val bvec = Spark2BreezeConverter.defaultSpark2BreezeConverter.convert(vec) //       .toArray.map(_*(v._1.toSeq(0))))
         val b = bvec :* scaling
          Breeze2SparkConverter.defaultBreeze2SparkVector.convert(b)  //return vector
         }
       //   IndexedRow(k, multiRes.toVector)
        (k, multiply(vw._2.vector,vw._1))  //IndexedRow 
    }.sortByKey(true)
   
    //  zMatUpRdd.take(5).foreach(println)
      
      val ArrayD2RDD = colSums.map{
        case e =>(e._1,
      //  Array.fill[Double](coo_matrix.numCols.toInt)(1/(Math.sqrt(e._2)))) 
      1.0/(Math.sqrt(e._2)))
      }
      
    //   ArrayD2RDD.take(5).foreach(println)
    
    //get multiply D2 diagnaol to V
        val vRdd = rddV.map{
        case r => (r.index, IndexedRow(r.index,r.vector))
      }
      
      val groupedzMatDownRdd = ArrayD2RDD.join(vRdd)      //cogroup (k, (Iterable<V>, Iterable<W>))

     val zMatDownRdd = groupedzMatDownRdd.map{
        case (k, vw) => 

         def multiply(vec :Vector, scaling :Double) :Vector = {
          val bvec = Spark2BreezeConverter.defaultSpark2BreezeConverter.convert(vec) //       .toArray.map(_*(v._1.toSeq(0))))
         val b = bvec :* scaling
          Breeze2SparkConverter.defaultBreeze2SparkVector.convert(b)  //return vector
         
       //   IndexedRow(k, multiRes.toVector)
        }
        (k, multiply(vw._2.vector, vw._1))    // no IndexedRow
            //iterator to list???
    }.sortByKey(true)
    
  //  println("354-----  ")  //IndexedRow.sortByKey(true)
  //  zMatDownRdd.take(5).foreach(println)
    val zMatIndexRdd = zMatUpRdd.union(zMatDownRdd)
   // println("355-----  ")
  //  zMatIndexRdd.take(5).foreach(println)
    zMatIndexRdd.saveAsTextFile("output/zMatrix")
    zMatIndexRdd
    
}

   //transfer rdd to RDD[vector]
  def IndexRowRddToRddVector(indRwMat :RDD[IndexedRow]) = {
    
    indRwMat.map{
      case v => v.vector
    }
  }
  
  //(key, vector) to rdd vector
  def PairRowRddToRddVector(indRwMat :RDD[(Long, Vector)]) = {
    
    indRwMat.map{
      case (k, v) => v
    }
  }
    
  //Do k means clustering
  def kMeansCluster(edglistCombine: (RDD[String],RDD[String],RDD[(String, (String, Double))],RDD[(String, Iterable[(String, Long)])],RDD[(String, Iterable[(String, Long)])]), sc :SparkContext, numClusters :Int) = {

    val colAFinalRdd = edglistCombine._1
    val colBFinalRdd = edglistCombine._2
    val edgeListRdd = edglistCombine._3
    val bipartCombineRddStoreA = edglistCombine._4
    val bipartCombineRddStoreB = edglistCombine._5
    //get IndexedRowMatrix
    val  resTransfer = readTableDataCombineNodes.egeListRddToIndexRowMatrix(colAFinalRdd, colBFinalRdd, edgeListRdd)
    
    val indRowmat = resTransfer._1
    val rowIndexRdd = resTransfer._2
    val colIndexRdd = resTransfer._3
    val m = indRowmat.numRows()
    val n = indRowmat.numCols()
    println("m : ", m, "n :", n)
    val maxIterations = 300
    val runs = 10
    val initalizeModel = "k-means||"
    val randomSeed = 0
    val MatrixAns = normalizeMatrixA(indRowmat)               //later use by another action?
    
    val n_discard = 1
    val zMatIndexRdd = getVectorForCluster(indRowmat, MatrixAns._1, MatrixAns._2, numClusters, n_discard, sc)       //persist or not
 
     val zMatVecRdd = PairRowRddToRddVector(zMatIndexRdd).persist(MEMORY_AND_DISK_SER)
    // KMeans.setInitializationMode(initalizeModel)
    val cocluster = KMeans.train(zMatVecRdd, numClusters, maxIterations, runs, initalizeModel, randomSeed)
   // cocluster.clusterCenters.foreach(println)
    val WSSSE = cocluster.computeCost(zMatVecRdd)
    println("Within Set Sum of Squared Errors = " + WSSSE)
     //cocluster.save(sc, "myModelPath")
    getPredict(cocluster, zMatVecRdd, m, n, rowIndexRdd, colIndexRdd, sc)
  }
  
  //get prediction
  def getPredict(cocluster: KMeansModel, zMatVecRdd: RDD[Vector], m: Long, n: Long, rowIndexRdd: RDD[(Long, String)], colIndexRdd: RDD[(Long, String)], sc: SparkContext) = {
      val clusterRdd  = cocluster.predict(zMatVecRdd)
    
    //clusterRdd.take(5).foreach(println)
   // println(" 452 --- ") 
     clusterRdd.zipWithIndex.map{
       case(v, k)=>(k, v)
     }.saveAsTextFile("output/clusterlabels")
    //get rowlableRdd
    val rowLabelsRdd =  clusterRdd.zipWithIndex.filter {
    _ match {
        case (v,k) => k < m
        case _ => false //incase of invalid input
    }
}.map{
    case (v, k)=> v
}.zipWithIndex.map{                         //  (cluster_no, index)
       case (v, k)=>(k.toLong, v.toInt)     //(index, cluster_no)
     }                     
     
   // val colLabelsRdd = clusterRdd.take(n.toInt)
   val colLabelsRdd=  clusterRdd.zipWithIndex.filter {
    _ match {
        case (v,k) => k >=  m
        case _ => false //incase of invalid input
    }
}.map{
    case (v, k)=> v
}.zipWithIndex.map{                         //  (cluster_no, index)
       case (v, k)=>(k.toLong, v.toInt)     //(index, cluster_no)
     }  
   
  //get the original dataset's cluster  
   // println("row label:  ")
  //  rowLabelsRdd.take(5).foreach(println)
   // println("column label:  ")
  //  colLabelsRdd .take(5).foreach(println)
   /*  
   val rowIndexRddOrder = rowIndexRdd.map{
  case (k, v) => (v.toInt, k)
}
   val colIndexRddOrder = colIndexRdd.map{
  case (k, v) => (v.toInt, k)
}
  */
   // println("504 --- ")
   val orgRowDataClusterRdd = rowIndexRdd.join(rowLabelsRdd).map{
     
      case (i, (f, l)) =>(l, f)
   }.sortByKey(true)
  // orgRowDataClusterRdd .take(5).foreach(println)
  
    val sqlContext = new SQLContext(sc) 
    import sqlContext.implicits._
    val schemaString = "cluster_no recordValue"
  // val schema = StructType(
   // schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
     val schemaStrArr = schemaString.split(" ")
     val schema =  StructType(
     StructField(schemaStrArr(0), IntegerType, true) ::
     StructField(schemaStrArr(1), StringType, true) :: Nil)

    val rowLabelRDD = orgRowDataClusterRdd.map(p => Row(p._1, p._2))
    val rowLabelDF = sqlContext.createDataFrame(rowLabelRDD, schema)
     rowLabelDF.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save("output/finalIDLabelResultRow.csv")

  val orgColumnDataClusterRdd = colIndexRdd.join(colLabelsRdd).map{
      case (i, (p, l)) =>(l, p)
   }.sortByKey(true)
   
  val ColumnLabelRDD = orgColumnDataClusterRdd.map(p => Row(p._1,  p._2))
    val ColumnLabelDF = sqlContext.createDataFrame(ColumnLabelRDD, schema)
     ColumnLabelDF.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save("output/finalIDLabelResultColumn.csv")
     
  }    
}
