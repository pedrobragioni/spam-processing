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

object CountAttach {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  var html = 0
  var plain = 0

  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count Attach").
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
        //mail.All_Parts_Content_Type.iterator
        mail.All_Parts_Content_Type.toList.distinct.iterator
      case _ => Iterator.empty
    }
    
    var attach = msgs.map(x => (x, 1))
                      .reduceByKey(_+_)
                      .map(x => x.swap)
                      .sortByKey(false)
                      .map(x => x.swap)
      
    val outputFile = inputFile.split ("/").last
    attach.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".attach")

    sc.stop()
  }

}
