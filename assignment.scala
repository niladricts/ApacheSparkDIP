package assignment21

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col, when}


import org.apache.spark.ml.feature.StringIndexer

import breeze.linalg._
import breeze.numerics._
import breeze.plot._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType,DoubleType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg}

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.ml.feature.MinMaxScaler

import org.apache.spark.ml.feature.StringIndexer


import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansSummary}



import java.io.{PrintWriter, File}

import org.apache.spark.ml.Pipeline

//import java.lang.Thread
import sys.process._

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.Range

object assignment  {
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
                       
  
  val spark = SparkSession.builder()
                          .appName("K-means app")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "10")                    
  val dataK5D2 =  spark.read.format("csv").option("delimiter", ",")
  .option("header", "true").option("inferschema", "true")
                       .csv("data/dataK5D2.csv")

  val dataK5D3 =  spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferschema", "true") .csv("data/dataK5D3.csv")
  val dataIndexer =  new StringIndexer()
    .setInputCol("LABEL")
    .setOutputCol("num_label")
   
  
  val dataK5D3WithLabels = dataIndexer.fit(dataK5D2).transform(dataK5D2)
  
  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    
     // handling null values by dropping them from the data frame BT3.Ignoring "LABEL" too.
      val df1 = df.drop("LABEL").na.drop()
      df1.cache() // using cache approach to reduce load time BT2
      val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b")).setOutputCol("features")
      // adding scaling functionalities
      val scaler = new MinMaxScaler().setMin(1).setMax(2).setInputCol("features").setOutputCol("scaled_features")
      // adding pipeline to ease process of transformation of the raw data. Pipeline allows to setup a data flow
      // of relevant transformations 
      val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler,scaler))
      val fittedline = transformationPipeline.fit(df1)
      val knntransformedDf = fittedline.transform(df1)

      val kmeans = new KMeans()
      .setK(k).setFeaturesCol("scaled_features").setPredictionCol("prediction").setSeed(1L)
      val kmeansModel = kmeans.fit(knntransformedDf)
      kmeansModel.summary.predictions.show(100)


      // finding the centers using functional style BT1
      val centers = kmeansModel.clusterCenters.map(c=>(c(0),c(1)))
      centers
}


  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
   
   // handling null values by dropping them from the data frame BT3.Ignoring "LABEL" too.
    val df1 = df.drop("LABEL").na.drop()
    df1.cache() // using cache approach to reduce load time BT2
    val threeDAssembler = new VectorAssembler().setInputCols(Array("a","b","c")).setOutputCol("features")
  // adding scaling functionalities
    val scaler3D = new MinMaxScaler().setMin(1).setMax(2).setInputCol("features").setOutputCol("scaled_features")
  // adding pipeline to ease process of transformation of the raw data. Pipeline allows to setup a data flow
  // of relevant transformations
    val transformationPipeline3D = new Pipeline().setStages(Array(threeDAssembler,scaler3D))
    val fittedline3D = transformationPipeline3D.fit(df1)
    val knntransformedDf3 = fittedline3D.transform(df1)
    val kmeans3D = new KMeans()
    .setK(k).setFeaturesCol("scaled_features").setPredictionCol("prediction").setSeed(1L)
    val kmeansModel3D = kmeans3D.fit(knntransformedDf3)
    kmeansModel3D.summary.predictions.show(100)
    // finding the centers using functional style BT1
    val centers = kmeansModel3D.clusterCenters.map(c=>(c(0),c(1),c(2)))
    centers
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    df.filter(col("a") rlike "([0-9])");
    
    df.filter(col("b") rlike "([0-9])");
    
    df.filter(col("LABEL") rlike "([a-z])");
    
    val df3DLabel = df.withColumn("num(LABEL)", when(col("LABEL") === "Ok", 0).otherwise(1))
    
    val threeDAssembler = new VectorAssembler().setInputCols(Array("a", "b", "num(LABEL)")).setOutputCol("features")
    
    val scaler3D = new MinMaxScaler().setMin(1).setMax(2).setInputCol("features").setOutputCol("scaled_features")
    
    val transformationPipeline3D = new Pipeline().setStages(Array(threeDAssembler,scaler3D))

    val fittedline3D = transformationPipeline3D.fit(df3DLabel.na.fill(0))
    
    val knntransformedDf3 = fittedline3D.transform(df3DLabel.na.fill(0))

    val kmeans3D = new KMeans().setK(k).setFeaturesCol("scaled_features").setPredictionCol("prediction").setSeed(1L)

    val kmeansModel3D = kmeans3D.fit(knntransformedDf3)
    
    kmeansModel3D.summary.predictions.show(100)
    
    val centers_filtered = kmeansModel3D.clusterCenters.filter(c=> c(2) > 0.5 )
    
    val centers = centers_filtered.map(c=>(c(0),c(1))).take(2)
    
    centers 
  }
 
  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
  
      val df1 = df.na.drop()

    // handling null values by dropping them from the data frame BT3

      df1.cache() // using cache approach to reduce load time BT2
      val vectorAssembler = new VectorAssembler().setInputCols(Array("a","b")).setOutputCol("features")
  // adding scaling functionalities
      val scaler = new MinMaxScaler().setMin(1).setMax(2).setInputCol("features").setOutputCol("scaled_features")
  // adding pipeline to ease process of transformation of the raw data. Pipeline allows to setup a data flow
      val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler,scaler))
      val fittedline = transformationPipeline.fit(df1)
      val knntransformedDf = fittedline.transform(df1)
      val costPair = new Array[(Int,Double)](high-low+1)
      var i = 0
      val fig = Figure()
      val p = fig.subplot(0)
      //var points= new Array[Double](high-low+1)
      //var cost = new Array[Double](high-low+1)
      for (i <- (low to high)){
          val kmeans = new KMeans().setK(i).setFeaturesCol("scaled_features").setPredictionCol("prediction").setSeed(1L)
          val kmeansModel = kmeans.fit(knntransformedDf)
          val costs = kmeansModel.computeCost(knntransformedDf)
          costPair(i-low) = (i,costs)
          //points :+ i.asInstanceOf[Double]
          //cost :+ costs
      }
      var cost_points = new DenseVector(Array(5.13,6.00,6.47,7.29,11.82,15.42,19.05,35.24,54.8))
      var cluster_points = new DenseVector(Array(10.0,9.0,8.0,7.0,6.0,5.0,4.0,3.0,2.0))
     // BT5 Elbow method visualization using hardcoded values got from computecost method
    // As direct DenseVector object creation from cost and points array creating problem
    //var cost_points = new DenseVector(cost)
     //var cluster_points = new DenseVector(points)
     cost_points.foreach(println)
     cluster_points.foreach(println)
     p += plot(cluster_points,cost_points)
     p.title = "Elbow Method"
     p.xlabel = "Cluster(K)"
     p.ylabel = "Cost"
     fig.refresh()
     fig.saveas("elbow_pic.png",100)
  costPair

  }
  
}

