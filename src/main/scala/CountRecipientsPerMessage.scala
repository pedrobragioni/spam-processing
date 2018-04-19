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

object CountRecipientsPerMessage {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"

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
        (mail.Rcpt_to.iterator.size, 1)
      case _ => (0, 1)
    }.reduceByKey(_ + _)

    val outputFile = inputFile.split ("/").last
    msgs.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".rcptsPerMsg")

    sc.stop()
  }

}
