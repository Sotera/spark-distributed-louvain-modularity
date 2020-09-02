package com.soteradefense.dga.graphx.louvain

import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.Array.canBuildFrom
import org.apache.spark.rdd.RDD
import java.io.{PrintWriter, File, FileOutputStream}

/**
  * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
  * Can also save locally if in local mode.
  *
  * See LouvainHarness for algorithm details
  */
class HDFSLouvainRunner(minProgress: Int, progressCounter: Int, outputdir: String) extends LouvainHarness(minProgress: Int, progressCounter: Int) {

  var qValues = Array[(Int, Double)]()

  override def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {
    graph.vertices.saveAsTextFile(outputdir + "/level_" + level + "_vertices")
    graph.edges.saveAsTextFile(outputdir + "/level_" + level + "_edges")
    graph.vertices.map({ case (id, v) => (id + "," + v.community)}).saveAsTextFile(outputdir + "/level_" + level + "_cluster")
    //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")
    qValues = qValues :+ ((level, q))
    println(s"qValue: $q")

    // overwrite the q values at each level
    sc.parallelize(qValues, 1).saveAsTextFile(outputdir + "/qvalues")
  }

  override def finalSave(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Double]) = {
    println("Merging output")
    var clusterMap: Map[Long, Long] = Map()
    var parentClusterMap: Map[Long, Long] = Map()

    // prepare last level map
    println(s"On level=$level")

    // Read the data as an RDD[(Long, Long)]
    var clusterOutputRDD: RDD[(Long, Long)] = sc.textFile(outputdir + "/level_" + level + "_cluster")
      .map(line => line.split(","))
      .map { case Array(x, y) => (x.toLong, y.toLong) }
    val clusterOutputList: List[(Long, Long)] = clusterOutputRDD.collect().toList

    var out = null

    for (out <- clusterOutputList) {
      val id: Long = out._1
      val clusterId: Long = out._2
      clusterMap += (id -> clusterId)
    }

    // iterate on all levels and combine output
    var l: Int = -1
    for (l <- (level - 1) to 0 by -1) {
      println(s"On level=$l")

      // switch parent and child map
      parentClusterMap.clear()
      parentClusterMap ++= clusterMap
      clusterMap.clear()

      var clusterOutputRDD: RDD[(Long, Long)] = sc.textFile(outputdir + "/level_" + level + "_cluster")
        .map(line => line.split(","))
        .map { case Array(x, y) => (x.toLong, y.toLong) }
      val clusterOutputList: List[(Long, Long)] = clusterOutputRDD.collect().toList

      for (out <- clusterOutputList) {
        val id: Long = out._1
        val clusterId: Long = parentClusterMap(out._2)
        clusterMap += (id -> clusterId)
      }
    }

    val pw = new PrintWriter(new File(outputdir + "/output.csv"))
    clusterMap.foreach(e => pw.println(e._1 + "," + e._2))
    pw.close()
  }

}