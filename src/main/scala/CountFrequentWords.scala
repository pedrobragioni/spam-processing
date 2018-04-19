/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import java.util.Vector

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import spam.Mail
import spam.reader.PatternInputFormat

object CountFrequentWords {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  var html = 0
  var plain = 0

  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count Frequent Words").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val stopwords_sc = sc.textFile("/stopwords").map (l => l)
    var stopwords = stopwords_sc.collect()

    val msgs = hf.flatMap {
      case (_,mail) if mail.From != "Invalid" =>
        mail.Content.iterator zip mail.Parts_Content_Type.iterator
      case _ => Iterator.empty
    }

    var plain_msgs = msgs.filter {case (content, t) => t == "text/plain"}
      .flatMap{ case  (content, t) => getText(content, stopwords).iterator }

    val html_msgs = msgs.filter {case (content, t) => t == "text/html"}
      //.map{ case (content, t) => getHtmlText(content, stopwords) }
      .flatMap{ case (content, t) => getHtmlText(content, stopwords).iterator }
      
    val union_msgs = html_msgs.union(plain_msgs).filter(_.nonEmpty).filter(_ != "")
      .map (w => (w, 1))
      .reduceByKey((a, b) => a + b)

    //val top10 = union_msgs.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))
    val freqWords = union_msgs.map(x => x.swap)
                              .sortByKey(false)
                              .map(x => x.swap)

    //for (t <- top10){
    //  println(t)
    //}

    //val ct_aux = html_msgs.collect()
    //ct_aux.foreach { println }

    val outputFile = inputFile.split ("/").last
    freqWords.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".freqwords")

    sc.stop()
  }

  def getText(plain: String, stopwords: Array[String]): ArrayBuffer[String] = {
    var rWords = ArrayBuffer[String]()

    val plain_ = plain.replaceAll("\\n", " ")

    for (w <- plain_.toLowerCase.split(" |\u00a0")){
      if (!stopwords.contains(w)){
        if(w.slice(0,4) != "http"){ 
          rWords += w.replaceAll("[,\\-#!?:<>|]", "").replaceAll("[.]$", "")
        }
      }
    }

    return rWords
  }

  def getHtmlText(raw_html: String, stopwords: Array[String]): ArrayBuffer[String] = {
    val html: Document = Jsoup.parse(raw_html)
    var words =  html.body().text().toLowerCase().split(" |\u00a0")
    var rWords = ArrayBuffer[String]()

    for (w <- words){
      if (!stopwords.contains(w)){
        if(w.slice(0,4) != "http"){ 
          rWords += w.replaceAll("[,\\-#!?:<>|]", "").replaceAll("[.]$", "")
        }
      }
    }

    return rWords
  }

}
