/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable

import scala.collection.JavaConversions._
import java.util.Vector
import spam.Mail
import spam.reader.PatternInputFormat

object CountContentType {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  var html = 0
  var plain = 0
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count Content Type").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val ct = hf.filter (_._2.From != "Invalid")
               .map {case (_,mail) => (getContentType(mail.All_Parts_Content_Type), 1) }
               .reduceByKey ((a, b) => a + b)

    //val ct = hf.flatMap {
    //  case (_,mail) if mail.From != "Invalid" =>
    //    mail.All_Parts_Content_Type.iterator zip Iterator.continually(1)
    //  case _ => Iterator.empty
    //}.reduceByKey (_ + _)

    //val ct_aux = ct.collect()
    //ct_aux.foreach { println }

    val outputFile = inputFile.split ("/").last
    ct.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".cts." + System.currentTimeMillis)

    sc.stop()
  }

  def getContentType(parts: Vector[String]) : String = {
    var _type = ""
    var _html = 0
    var _plain = 0

    for (p <- parts){
      if (p == "text/html"){
        _html += 1 
      } else if (p == "text/plain"){
        _plain += 1
      } 
    }

    if (_html == 0 && _plain > 0){
      _type = "plain"
    } else if (_html > 0 && _plain == 0){
      _type = "html"
    } else if (_html > 0 && _plain > 0){ 
      _type = "mixed"
    }

    return _type
  }
}
