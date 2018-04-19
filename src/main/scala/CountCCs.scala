/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object CountCCs {
  val regex = "^From\\s.*\\s*\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"
  def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count CCs").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)
    //val ccs = hf.filter (_._2.From != "Invalid")
    //            .map { case (_,mail) => (mail.Src_CC, 1) }
    //            .reduceByKey ((a, b) => a + b)

    val proto = hf.filter (_._2.From != "Invalid")
                .map { case (_,mail) => (mail.Src_Proto.split(" ")(1), mail.Src_CC) }
                .distinct
                .groupByKey()
                .map (x => (x._1, x._2.toList.distinct.length))
                
    System.gc()

    //println ("Number of distinct CC's = " + ccs.count)
    val outputFile = inputFile.split ("/").last
    //proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ccs_protocol." + System.currentTimeMillis)
    proto.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".ccs_protocol")
    
    //val out = new java.io.FileWriter(outputFile+".total_ccs")
    //out.write(ccs.count.toString)
    //out.close()


    sc.stop()
  }
}
