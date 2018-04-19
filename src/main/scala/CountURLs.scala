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

object CountURLs {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  var html = 0
  var plain = 0
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count URLs").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val msgs = hf.flatMap {
      case (_,mail) if mail.From != "Invalid" =>
        mail.Content.iterator zip mail.Parts_Content_Type.iterator
      case _ => Iterator.empty
    }

    val plain_msgs = msgs.filter {case (content, t) => t == "text/plain"}
      .flatMap{ case (content, t) => getPlainURLs(content) }
      .map { case w => (w, 1) }

    val html_msgs = msgs.filter {case (content, t) => t == "text/html"}
      .flatMap{ case (content, t) => getHtmlURLs(content) } 
      .map { case w => (w, 1) }
      .union(plain_msgs)
      .reduceByKey(_+_)
      .map(x => x.swap)
      .sortByKey(false)
      .map(x => x.swap)

    //val ct_aux = html_msgs.take(100)
    //ct_aux.foreach { println }

    val outputFile = inputFile.split ("/").last
    html_msgs.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".urls")

    sc.stop()
  }

  def getPlainURLs(raw: String): ArrayBuffer[String] = {
    var rWords = ArrayBuffer[String]()

    for (w <- raw.split(" |\u00a0")){
      if(w.slice(0,4) == "http"){
        rWords += w
      }
    }

    return rWords
  }

  def getHtmlURLs(raw_html: String): ArrayBuffer[String] = {
    var rWords = ArrayBuffer[String]()

    //var words = ""
    val html: Document = Jsoup.parse(raw_html)
    var link = html.select("a")
    if(link != null){
      link.first()
      rWords += link.attr("href")
    }
    //var words =  html.body().text().toLowerCase().split(" ")
    return rWords
  }

}
