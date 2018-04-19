/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object Top10_ips {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Top10").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)

    val msgs = hf.filter (_._2.From != "Invalid").cache()

    val ips = msgs.map { case (_,mail) => (mail.Src_IP, 1) }
                .reduceByKey ((a, b) => a + b)
                //.map(x => x.swap)
                .takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))


    //println ("Number of distinct IP's = " + ips.count)
    //for (i <- ases){
      //println(i)
    //}

    val outputFile = inputFile.split ("/").last
    val outIPs = new java.io.FileWriter(outputFile+".top10.ips")

    for (i <- ips){
      outIPs.write(i.toString+"\n")
    }
    outIPs.close()

    sc.stop()
  }
}
