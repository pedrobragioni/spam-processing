/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable

import scala.collection.JavaConversions._
import spam.Mail
import spam.reader.PatternInputFormat

object CountRecipientsTwo {
  val regex = "^From\\s.*\\s\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count Recipients Two").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val rcpts = hf.flatMap {
      case (_,mail) if mail.From != "Invalid" =>
        mail.Rcpt_to.iterator zip Iterator.continually (1)
      case _ => Iterator.empty
    }.reduceByKey (_ + _, 32)

    rcpts.saveAsTextFile (inputFile + ".rcpts." + System.currentTimeMillis)

    sc.stop()
  }
}
