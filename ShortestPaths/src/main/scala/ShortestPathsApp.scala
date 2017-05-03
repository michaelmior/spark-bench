
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

import java.io.PrintWriter

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


object ShortestPathsApp {

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: <input> <output> <minEdge> <numV> <StorageLevel>")
      System.exit(0)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf
    conf.setAppName("Spark ShortestPath Application")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)
    val minEdge= args(2).toInt
    val numVertices= args(3).toInt
    val storageLevel= args(4)

    var sl:StorageLevel = StorageLevel.fromString(storageLevel)
    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, sl, sl)
    graph.edges.setName("SPEdges")
    graph.vertices.setName("SPVertices")

    val landmarks = Seq(1, numVertices).map(_.toLong)
    var start = System.currentTimeMillis();
    val results=ShortestPaths.run(graph,landmarks).vertices
    val computeTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    results.saveAsTextFile(output)

    val jsonStr = compact(render(Map("computeTime" -> computeTime)))
    new PrintWriter("out/" + storageLevel + "-" + numVertices + ".json") { write(jsonStr); close }

    sc.stop();
  }
}
