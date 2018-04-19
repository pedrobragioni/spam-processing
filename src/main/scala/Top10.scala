/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object Top10 {
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

    val cc_msgs = msgs.map { case (_,mail) => (mail.Src_CC, 1) }
                .reduceByKey ((a, b) => a + b)
                //.map(x => x.swap)
                //.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))

    val cc_ips = msgs.map{ case (_,mail) => (mail.Src_CC, mail.Src_IP) }
                    .distinct()
                    .groupByKey()
                    .map (x => (x._1, x._2.toList.distinct.length))

     System.gc()

     val cc_ases = msgs.map{ case (_,mail) => (mail.Src_CC, mail.Src_ASN) } 
                    .distinct()
                    .groupByKey()
                    .map (x => (x._1, x._2.toList.distinct.length))

    System.gc()

    val ccs = cc_msgs.join(cc_ips).join(cc_ases)
                  .map(x => (x._1, x._2._1._1, x._2._1._2, x._2._2))
                  .takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))

    
    val as_msgs = msgs.map { case (_,mail) => (mail.Src_ASN, 1) }
                .reduceByKey ((a, b) => a + b)
                //.map(x => x.swap)
                //.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))

    System.gc()

    val as_ips = msgs.map{ case (_,mail) => (mail.Src_ASN, mail.Src_IP) } 
                    .distinct()
                    .groupByKey()
                    .map (x => (x._1, x._2.toList.distinct.length))

    System.gc()

    val ases = as_msgs.join(as_ips)
                  .map(x => (x._1, x._2._1, x._2._2))
                  .takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))

    val ips = msgs.map { case (_,mail) => (mail.Src_IP, 1) }
                .reduceByKey ((a, b) => a + b)
                //.map(x => x.swap)
                .takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))


    //println ("Number of distinct IP's = " + ips.count)
    //for (i <- ases){
      //println(i)
    //}

    val outputFile = inputFile.split ("/").last
    val outCCs = new java.io.FileWriter(outputFile+".top10.ccs")

    for (c <- ccs){
      outCCs.write(c.toString+"\n")
    }
    outCCs.close()

    val outAses = new java.io.FileWriter(outputFile+".top10.ases")

    for (a <- ases){
      outAses.write(a.toString+"\n")
    }
    outAses.close()

    val outIPs = new java.io.FileWriter(outputFile+".top10.ips")

    for (i <- ips){
      outIPs.write(i.toString+"\n")
    }
    outIPs.close()

    sc.stop()
  }
}
