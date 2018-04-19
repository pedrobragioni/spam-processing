/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object CountMessages {
  //val regex = "^From\\s.*\\s\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  val regex = "^From\\s.*\\s*[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count Messages").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)
    
    //val msgs = hf.filter (_._2.From != "Invalid")
    //val proto = msgs.map { case (_,mail) => (mail.Src_Proto.split(" ")(1), 1) }.reduceByKey ((a, b) => a + b)

    //System.gc()

    println ("Number of messages = " + hf.filter (_._2.From != "Invalid").count)
    //val outputFile = inputFile.split ("/").last
    //proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".protocols." + System.currentTimeMillis)
    //proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".msgs_protocols")

    //val out = new java.io.FileWriter(outputFile+".total_messages")
    //out.write(msgs.count.toString)
    //out.close()

    sc.stop()
  }
}
