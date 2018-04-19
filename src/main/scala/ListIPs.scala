/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object ListIPs {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count IPs").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)
    //val ips = hf.filter (_._2.From != "Invalid")
    //            .map { case (_,mail) => (mail.Src_IP, 1) }
    //            .reduceByKey ((a, b) => a + b)

    val http_ips = hf.filter (_._2.From != "Invalid").filter(_._2.Src_Proto.split(" ")(1) == "HTTP")
                .map { case (_,mail) => mail.Src_IP }
                .distinct()

    System.gc()

    //println ("Number of distinct IP's = " + ips.count)
    val outputFile = inputFile.split ("/").last
    //proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ips_protocol." + System.currentTimeMillis)
    http_ips.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".http_ips")

    //val out = new java.io.FileWriter(outputFile+".total_ips")
    //out.write(ips.count)
    //out.close()

    sc.stop()
  }
}
