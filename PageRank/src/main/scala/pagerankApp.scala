/*
 * (C) Copyright IBM Corp. 2015
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package src.main.scala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{ SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.impl.{ EdgePartitionBuilder, GraphImpl }
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object pagerankApp {

  def main(args: Array[String]) {
    if (args.length < 5) {
      println("usage: <input> <output> <minEdge> <maxIterations> <tolerance> <resetProb> <StorageLevel>")
      System.exit(0)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf
    conf.setAppName("Spark PageRank Application")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)
    val minEdge = args(2).toInt
    val maxIterations = args(3).toInt
    val tolerance = args(4).toDouble
    val resetProb = args(5).toDouble
    val storageLevel=args(6)

    var sl:StorageLevel = StorageLevel.fromString(storageLevel)
    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, sl, sl)



    var start = System.currentTimeMillis();
    val staticRanks = graph.staticPageRank(maxIterations, resetProb).vertices
    val computeTime = (System.currentTimeMillis() - start).toDouble / 1000.0
    staticRanks.saveAsTextFile(output);

    println(compact(render(Map("computeTime" -> computeTime))))
    sc.stop()
  }

  def pagerank_usingSampledata(sc: SparkContext, input: String, output: String,
                               maxIterations: Integer, tolerance: Double, resetProb: Double) {
    val graph = GraphLoader.edgeListFile(sc, input + "/followers.txt")

    val staticranks = graph.staticPageRank(maxIterations, resetProb).vertices

    val ranks = graph.pageRank(tolerance, resetProb).vertices

    // Join the ranks with the usernames
    val users = sc.textFile(input + "/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }
}
