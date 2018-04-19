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

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import spam.Mail
import spam.reader.PatternInputFormat

object PhishingAndLSH {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  var html = 0
  var plain = 0

  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Phish and LSH").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // Creating SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    // Get content and id for each message
    val messages = hf.map {
      case (_, mail) if (mail.From != "Invalid" && checkMessage(mail.Content, mail.Parts_Content_Type, mail.Rcpt_to.iterator.size)) =>
        (mail.Content, mail.Parts_Content_Type, mail.Src_IP, mail.Src_ASN, mail.Src_CC)
      case _ => (new Vector[String](), new Vector[String](), "", "", "")
    }.zipWithIndex

    // Return shingles for each message
    val shingles = messages.filter (x => (x._1._1.length > 0 && x._1._2.length > 0))
      .map(x => (getContent(x._1._1, x._1._2), x._2))
      .flatMap(x => x._1.map(y => (y, x._2)))

    System.gc()

    // Return all distinct shingles present in messages
    val shinglesVocab = shingles.map(x => x._1)
      .distinct
      .zipWithIndex

    val shinglesVocabSize = shinglesVocab.count.toInt
    val sparseShingles = shingles.join(shinglesVocab).map(x => (x._2._1, x._2._2)).groupByKey()
                                .map(y => (y._1, y._2.map(i => (i.toInt, 1.))))
                                .map(a => (a._1, Vectors.sparse(shinglesVocabSize, a._2.to[Seq]).asInstanceOf[SparseVector]))
    //val sparseShingles = shingles.map(x => (x._1, getSparseShingles(x._2, shinglesVocab))).filter(x => x._2.indices.length > 0).cache()
       
    System.gc()

    val numRows = 1000
    val primeNumber = 3061301
    val _hashFunctions = ListBuffer[Hasher]()
    for (i <- 0 until numRows)
      _hashFunctions += Hasher.create(primeNumber, 1000)
    val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex

    val minClusterSize = 10
    val numBands = 25

    val signatures = sparseShingles.flatMap(v => hashFunctions.flatMap(h => List(((v._1, h._2 % numBands),h._1.minhash(v._2)))))
    val bands = signatures.groupByKey().map(x => ((x._1._2, x._2.hashCode), x._1._1)).groupByKey().
                          filter(x => x._2.size >= minClusterSize).map(x => x._2.toList.sorted).distinct()

    val auxBands = bands.zipWithUniqueId.map(x => (x._2, x._1)).cache()

   val edges = auxBands.flatMap(s => s._2.map(i => (i, s._1)))
                        .groupByKey.flatMap(g => g._2.flatMap(x => g._2.map((x, _))))
                        .distinct()

    System.gc()

    val vertices = Graph.fromEdgeTuples[Long](edges, defaultValue = 0)
                        .connectedComponents.vertices
    val clusters = auxBands.join(vertices).map(x => (x._2._2, x._2._1))
                        .reduceByKey((s1, s2) => s1.union(s2).distinct)

    val clusters_msgs = clusters.flatMap(x => x._2.map(i => (i, x._1))).join(messages.map(x => (x._2, (x._1._3, x._1._4, x._1._5))))
                      .map(a => (a._2._1, (a._2._2._1, a._2._2._2, a._2._2._3))).groupByKey()

    System.gc()

    val result_msgs = clusters_msgs.map(x => (x._1, x._2.toList.length))
    val result_ips = clusters_msgs.map(x => (x._1, (x._2.map(i => i._1)))).map(x => (x._1, x._2.toList.distinct.length))
    val result_ases = clusters_msgs.map(x => (x._1, (x._2.map(i => i._2)))).map(x => (x._1, x._2.toList.distinct.length))
    val result_ccs = clusters_msgs.map(x => (x._1, (x._2.map(i => i._3)))).map(x => (x._1, x._2.toList.distinct.length))

    val result = result_msgs.join(result_ips).join(result_ases).join(result_ccs).map(x => (x._1, x._2._1._1._1, x._2._1._1._2, x._2._1._2, x._2._2))

    //val vectorCluster = auxBands.zipWithIndex().map(x => x._1.map(y => (y.asInstanceOf[Long], x._2))).flatMap(x => x.grouped(1)).map(x => x(0))
    
    //val clusters = messages.map(x => x.swap).join(vectorCluster).map(x => (x._2._2, x._2._1)).groupByKey()
    //val scores = clusters.map(row => (row._1, jaccard(row._2.toList))) 

    val outputFile = inputFile.split ("/").last
    //result_msgs.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".msgs." + System.currentTimeMillis)
    //result_ips.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ips." + System.currentTimeMillis)
    //result_ases.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ases." + System.currentTimeMillis)
    //result_ccs.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ccs." + System.currentTimeMillis)
    result.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".results." + System.currentTimeMillis) 

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

  def getContent(content: Vector[String], contentType: Vector[String]): Set[String] = {
    var count = 0
    var aContent = ArrayBuffer[String]()
    var wholeContent: String = ""

    if (!contentType.isEmpty){
      for (c <- content){
        if (contentType(count) == "text/html"){
          val html: Document = Jsoup.parse(c)
          val words =  html.body().text().toLowerCase().replaceAll("\u00a0", " ")

          wholeContent = wholeContent + words + "\n"

        } else if(contentType(count) == "text/plain"){
          wholeContent = wholeContent + c.toLowerCase() + "\n"
        }
        count+=1
      }

      wholeContent = wholeContent.replaceAll("[,#!?:<>|]", "")
      wholeContent = wholeContent.replaceAll("[.]$", "")
      wholeContent = wholeContent.replaceAll("\n"," ")
      wholeContent = wholeContent.replaceAll(" +"," ")
      return getShingles(wholeContent, 5)
    }
    else { return Set.empty[String] }
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

  /** Phishing identification methods */
  def checkMessage(content: Vector[String], parts_content_type: Vector[String], numRcpts: Int): Boolean = {
    var contentAndType = content zip parts_content_type
    var score = 0.0

    score += checkDirectMessage(numRcpts)

    for (c <- contentAndType.iterator){
      if( c._2 == "text/plain" ){
        score += getText(c._1)
      } else if( c._2 == "text/html" ){
        score += getHtmlText(c._1)
      }
    }

    if (score > 3.0)
      return true
    else
      return false
  }

  def getText(plain: String): Int = {
    var r = 0

    val plain_ = plain.replaceAll("\\n", " ")

    for (w <- plain_.toLowerCase.split(" |\u00a0")){
      r += checkMoneyMention(w)
      r += checkSenseOfUrgency(w)
      r += checkReplyInducingPresence(w)
    }

    return r
  }

  def getHtmlText(raw_html: String): Int = {
    val html: Document = Jsoup.parse(raw_html)
    var words =  html.body().text().toLowerCase().split(" |\u00a0")
    var r = 0

    for (w <- words){
      r += checkMoneyMention(w)
      r += checkSenseOfUrgency(w)
      r += checkReplyInducingPresence(w)
    }

    return r
  }

  def checkDirectMessage(numRcpts: Int): Int = {
    if(numRcpts == 1){
      return 1
    }
    return 0
  }

  def checkMoneyMention(word: String): Int = {
    if(word.slice(0,1) == "$" || word.slice(0,3) == "usd" || word.slice(0,3) == "us$"){
      return 1
    } else if(word.slice(0,2) == "r$" || word.slice(0,3) == "brl"){
      return 1
    } else if(word.slice(0,2) == "c$" || word.slice(0,3) == "cad"){
      return 1
    } else if(word.slice(0,1) == "£" || word.slice(0,3) == "gbp"){
      return 1
    } else if(word.slice(0,5) == "money" || word.slice(0,4) == "cash" || word.slice(0,5) == "dollar" || word.slice(0,5) == "prize" || word.slice(0,5) == "lottery"){
      return 1
    }

    return 0
  }

  def checkSenseOfUrgency(word: String): Int = {
    if(word.slice(0,6) == "urgent" || word.slice(0,3) == "now" ||
      word.slice(0,5) == "today" || word.slice(0,7) == "instant" ||
      word.slice(0,12) == "straightaway" || word.slice(0,6) == "direct" ||
      word.slice(0,4) == "once" || word.slice(0,9) == "desperate" ||
      word.slice(0,3) == "now" || word.slice(0,11) == "immediately" ||
      word.slice(0,4) == "soon" || word.slice(0,7) == "shortly" ||
      word.slice(0,5) == "quick" || word.slice(0,7) == "suspend"){
        return 1
    }

    return 0
  }

  def checkReplyInducingPresence(word: String): Int = {
    if(word.slice(0,5) == "write" || word.slice(0,7) == "contact" ||
      word.slice(0,5) == "reply" || word.slice(0,8) == "response" || 
      word.slice(0,7) == "forward" || word.slice(0,4) == "back" ||
      word.slice(0,4) == "send" || word.slice(0,4) == "hear"){
        return 1
      }

    return 0
  }

}
