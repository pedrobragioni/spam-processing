/**
 * Identificar caracteristicas comuns de phishings. Sem necessidade de avaliar
 * URL.
 * Baseado no artigo: "Identification and Detection of Phishing Emails using
 * Natural Language Processing Techniques"
 **/

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

object CheckPhishingCharacteristics {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  var html = 0
  var plain = 0

  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Check Phishing Characteristics").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val msgs = hf.map {
      case (_,mail) if mail.From != "Invalid" =>
        (checkMessage(mail.Content, mail.Parts_Content_Type, mail.Rcpt_to.iterator.size), 1)
      case _ => (0.0, 1)
    }.reduceByKey(_ + _)

    val outputFile = inputFile.split ("/").last
    msgs.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".checkphish")

    sc.stop()
  }

  def checkMessage(content: Vector[String], parts_content_type: Vector[String], numRcpts: Int): Double = {
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

    return score
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
    //urgent 
    //suspend
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
