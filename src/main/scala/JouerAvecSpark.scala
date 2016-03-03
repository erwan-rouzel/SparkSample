import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by erwanrouzel on 03/03/2016.
  */
object JouerAvecSpark extends App {
  val conf = new SparkConf()
                  .setMaster("local[4]")
                  .setAppName("example")
  val sc = new SparkContext(conf)

  sc.textFile("/tmp/test.in3")
      .map(_.split(";"))
      .map((tokens:Array[String]) => (tokens(0), Float.unbox(tokens(9))))
      .groupBy(_._1)
      .foreach(println)

  sc.stop()

}
