/* CountMessages.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import collection.JavaConversions._
import java.util.Vector

// hadoop specif imports
import org.apache.hadoop.io.LongWritable
import spam.Mail
import spam.reader.PatternInputFormat

object CountMessagesWithURL {
  val regex = "^From\\s.*\\s\\s[A-Za-z]{3}\\s[A-Za-z]{3}\\s+\\d+\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}$"

  /*def main(args: Array[String]) {

    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Count Messages").
      set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      registerKryoClasses (Array(classOf[LongWritable], classOf[Mail]))
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("record.delimiter.regex", regex)

    val hf = sc.newAPIHadoopFile [LongWritable, Mail, PatternInputFormat] (inputFile)
    val msgs_content = hf.filter (_._2.From != "Invalid").map{ case (k, mail) => (processMsgContent(mail.Content, mail.Parts_Content_Type))} 

    //val processed_msgs = msgs_content.map{ case (mail, cont) =>  

    //val html_msgs = msgs_content.filter(_._2.Content.matches(".*\\<[^>]+>.*"))

    msgs_content.saveAsTextFile("hdfs://master:8022/teste." + System.currentTimeMillis) 

    sc.stop()
  }

  def processMsgContent(content: Vector[String], content_type: Vector[String]): String = {
    var str = ""
    var c = 0
    for (cont <- content){
      if(content_type(c) == "text/html"){
        str += "HTMLLLLLLLLL!!!!!!!\n"
      }
      else if(content_type(c) == "text/plain"){
        str += "NORMALLLLLLLLLLLLL!!!!!!\n"
      }
      c+=1 
    }
    return str
  }*/

}
