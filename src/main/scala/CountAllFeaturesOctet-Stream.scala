/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object CountAllFeaturesOctetStream {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count All Features").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val m = hf.filter (_._2.From != "Invalid").filter(_._2.All_Parts_Content_Type.contains("application/octet-stream"))
    
    val msgs_proto = m.map { case (_,mail) => (mail.Src_Proto.split(" ")(1), 1) }.reduceByKey ((a, b) => a + b)

    System.gc()

    //val ips = m.map { case (_,mail) => (mail.Src_IP, 1) }
    //           .reduceByKey ((a, b) => a + b)

    val ips_proto = m.map { case (_,mail) => (mail.Src_Proto.split(" ")(1), mail.Src_IP) }
                     .groupByKey()
                     .map (x => (x._1, x._2.toList.distinct.length))

    System.gc()

    //val ases = m.map { case (_,mail) => (mail.Src_ASN, 1) }
    //            .reduceByKey ((a, b) => a + b)

    val ases_proto = m.map { case (_,mail) => (mail.Src_Proto.split(" ")(1), mail.Src_ASN) }
                      .groupByKey()
                      .map (x => (x._1, x._2.toList.distinct.length))
     
    System.gc()

    //val ccs = m.map { case (_,mail) => (mail.Src_CC, 1) }
    //           .reduceByKey ((a, b) => a + b)

    val ccs_proto = m.map { case (_,mail) => (mail.Src_Proto.split(" ")(1), mail.Src_CC) }
                      .groupByKey()
                      .map (x => (x._1, x._2.toList.distinct.length))
     

    System.gc()

    //println ("Number of distinct Msgs = " + m.count)
    //println ("Number of distinct IP's = " + ips.count)
    //println ("Number of distinct ASes = " + ases.count)
    //println ("Number of distinct CC's = " + ccs.count)
    val outputFile = inputFile.split ("/").last
    msgs_proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".octet_msgs_protocol")
    ips_proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".octet_ips_protocol")
    ases_proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".octet_ases_protocol")
    ccs_proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".octet_ccs_protocol")

    sc.stop()
  }
}
