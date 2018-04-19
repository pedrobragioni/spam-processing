/* LSHDriver.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.{Vectors, SparseVector}

// hadoop specif imports
import org.apache.hadoop.io.LongWritable

import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Set
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import java.util.Vector

object LSHDriver_Linha {
  var html = 0
  var plain = 0

  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("LSH Driver")
    
    // Creating SparkContext
    val sc = new SparkContext(conf)

    val hf = sc.textFile(inputFile)

    // Get content and id for each message
    /*val messages = hf.map {
      case (_, mail) if mail.From != "Invalid" =>
        (mail.Content, mail.Parts_Content_Type)
      case _ => (new Vector[String](), new Vector[String]())
    }.zipWithIndex*/

   val messages = hf.map(x => x.toString()).zipWithIndex

    // Return shingles for each message
    val shingles = messages.map(x => (getShingles(x._1, 5), x._2))
      .flatMap(x => x._1.map(y => (y, x._2)))
    
    // Return all distinct shingles present in messages
    val shinglesVocab = shingles.map(x => x._1)
      .distinct
      .zipWithIndex

    val shinglesVocabSize = shinglesVocab.count.toInt
    val sparseShingles = shingles.join(shinglesVocab).map(x => (x._2._1, x._2._2)).groupByKey()
                                .map(y => (y._1, y._2.map(i => (i.toInt, 1.))))
                                .map(a => (a._1, Vectors.sparse(shinglesVocabSize, a._2.to[Seq]).asInstanceOf[SparseVector]))
    //val sparseShingles = shingles.map(x => (x._1, getSparseShingles(x._2, shinglesVocab))).filter(x => x._2.indices.length > 0).cache()
        
    val numRows = 1000
    val _hashFunctions = ListBuffer[Hasher]()
    for (i <- 0 until numRows)
      _hashFunctions += Hasher.create(65537, 1000)
    val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex

    val minClusterSize = 1
    val numBands = 25

    val signatures = sparseShingles.flatMap(v => hashFunctions.flatMap(h => List(((v._1, h._2 % numBands),h._1.minhash(v._2)))))
    val bands = signatures.groupByKey().map(x => ((x._1._2, x._2.hashCode), x._1._1)).groupByKey().
                          filter(x => x._2.size >= minClusterSize).map(x => x._2.toList.sorted).distinct()

    val auxBands = bands.zipWithUniqueId.map(x => (x._2, x._1)).cache()

   val edges = auxBands.flatMap(s => s._2.map(i => (i, s._1)))
                        .groupByKey.flatMap(g => g._2.flatMap(x => g._2.map((x, _))))
                        .distinct()
                        
    val vertices = Graph.fromEdgeTuples[Long](edges, defaultValue = 0)
                        .connectedComponents.vertices
    val teste = auxBands.join(vertices).map(x => (x._2._2, x._2._1))
                        .reduceByKey((s1, s2) => s1.union(s2).distinct)

   //val vectorCluster = auxBands.zipWithIndex().map(x => x._1.map(y => (y.asInstanceOf[Long], x._2))).flatMap(x => x.grouped(1)).map(x => x(0))
    
    //val clusters = messages.map(x => x.swap).join(vectorCluster).map(x => (x._2._2, x._2._1)).groupByKey()
    //val scores = clusters.map(row => (row._1, jaccard(row._2.toList))) 

    val outputFile = inputFile.split ("/").last
    teste.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".final." + System.currentTimeMillis)

    sc.stop()
  }

  def getSparseShingles(shingles: Set[String], vocab:Array[(Long, String)]): SparseVector = {
    var indices = ArrayBuffer[Int]()
    var values = ArrayBuffer[Double]()

    for (i <- vocab){
      if (shingles.contains(i._2)){
        indices += i._1.toInt
        values += 1.0
      }
    }

    val sparseShingles = Vectors.sparse(vocab.length, indices.toArray, values.toArray).asInstanceOf[SparseVector]

    return sparseShingles
  }

  def getShingles(content: String, k: Int): Set[String] = {
    //generate shingles using characters
    //val shingles = content.sliding(k).map(_.mkString).toSet
    
    //generate shingles using words
    val shingles = content.split(" ").sliding(k).map(x => x.mkString(" ")).toSet

    return shingles.toSet
  }

  /** compute jaccard between two vectors */
  def jaccard(a : SparseVector, b : SparseVector) : Double = {
    val al = a.indices.toList
    val bl = b.indices.toList
    al.intersect(bl).size / al.union(bl).size.doubleValue
  }

  /** compute jaccard similarity over a list of vectors */
  def jaccard(l : List[SparseVector]) : Double = {
    l.foldLeft(l(0).indices.toList)((a1, b1) => a1.intersect(b1.indices.toList.asInstanceOf[List[Nothing]])).size / 
    l.foldLeft(List())((a1, b1) => a1.union(b1.indices.toList.asInstanceOf[List[Nothing]])).distinct.size.doubleValue
  }  
}
