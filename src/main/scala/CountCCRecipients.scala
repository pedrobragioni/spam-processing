/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable

import scala.collection.JavaConversions._

import spam.Mail
import spam.reader.PatternInputFormat

object CountCCRecipients {
  val regex = "^From\\s.*\\s\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count CC Recipients").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val ccRcpts = hf.flatMap {
      case (_,mail) if mail.From != "Invalid" =>
        for (rcpt <- mail.Rcpt_to.iterator) yield (mail.Dst_CC, rcpt.toString)
      case _ => Iterator.empty
    }.distinct.mapValues (_ => 1).reduceByKey (_ + _)

    //val ccRcpts = hf.flatMap {
    //  case (_,mail) if mail.From != "Invalid" =>
    //    for (rcpt <- mail.Rcpt_to.iterator) yield (mail.Dst_CC, Set(rcpt.toString))
    //  case _ => Iterator.empty
    //}.reduceByKey (_ ++ _).mapValues (_.size)

    ccRcpts.saveAsTextFile (inputFile + ".cc-rcpt." + System.currentTimeMillis)
    

    sc.stop()
  }
}
