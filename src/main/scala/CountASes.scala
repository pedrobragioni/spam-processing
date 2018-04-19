/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object CountASes {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count ASes").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)
    //val ases = hf.filter (_._2.From != "Invalid")
    //            .map { case (_,mail) => (mail.Src_ASN, 1) }
    //            .reduceByKey ((a, b) => a + b)

    val proto = hf.filter (_._2.From != "Invalid")
                .map { case (_,mail) => (mail.Src_Proto.split(" ")(1), mail.Src_ASN) }
                .distinct()
                .groupByKey()
                .map (x => (x._1, x._2.toList.distinct.length))
                
    System.gc()

    //println ("Number of distinct ASes = " + ases.count)
    val outputFile = inputFile.split ("/").last
    //proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ases_protocol." + System.currentTimeMillis)
    proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ases_protocol")

    //val out = new java.io.FileWriter(outputFile+".total_ases")
    //out.write(ases.count)
    //out.close()

    sc.stop()
  }
}
