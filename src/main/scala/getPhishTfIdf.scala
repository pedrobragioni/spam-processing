import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf 
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.IDF

object getPhishTfIdf {

  def main(args: Array[String]) {

    val inputFile = args(0)
    val conf = new SparkConf().setAppName("Get Phish")
    val sc = new SparkContext(conf)

    val documents: RDD[Seq[String]] = sc.textFile(inputFile).map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()
    val idf = new IDF(minDocFreq = 2).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    //tfidf.map(x => println(x))
    val teste = tfidf.take(1)
    println("--------------------------"+ teste.take(1)(1))
    println("--------------------------" + hashingTF.indexOf("dear"))
    println("--------------------------" + hashingTF.indexOf("customer"))
    println("--------------------------" + hashingTF.indexOf("recently"))
    println("--------------------------" + hashingTF.indexOf("detected"))
    println("--------------------------" + hashingTF.indexOf("several"))
    println("--------------------------" + hashingTF.indexOf("failed"))
    println("--------------------------" + hashingTF.indexOf("dear"))

    //println("------------------------" + tfidf.take(1).count())

    //val outputFile = inputFile.split ("/").last 
    //tfidf.saveAsTextFile ("hdfs://master:8022/" + outputFile + ".tfidf")

    sc.stop()
  }

}
