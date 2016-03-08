import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by erwanrouzel on 03/03/2016.
  */
object JouerAvecSpark extends App {

  /* local[4] => 4 est le nb de threads
  *  Très bien documenté sur le site Spark
  * */

  val conf = new SparkConf()
                  .setMaster("local[4]")
                  .setAppName("example")

  val sc = new SparkContext(conf)

  sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000")

  /* sc.textFile("/tmp/test.in3") => Donne une RDD
   * .map( (line:String) => line.split(";") )
   *   => On peut utiliser le "_" qui est un "sucre synthaxique"
   *
   *   C'est une bonne pratique de faire des map les uns à la suite des autres
   *   car ils peuvent être alors optimisés
   *   */

  val cleanDataset = sc.textFile("hdfs:///tmp/test.in3")
      .map(_.split(";"))
      .map(tokens => RawDatapoint(tokens(0), tokens(9)))
      .map(datapoint => RawDatapoint(datapoint.time, datapoint.measure.replace("+", "")))
      .filter(_.measure.matches("""\d+"""))
      .map(raw => (raw.time, raw.measure.toFloat))
      .filter(_._2 <= 999.0)
      .cache()

  // On met le dataset en cache, on aurait aussi pu préciser la méthode de stockage :
  //cleanDataset.persist(StorageLevel.MEMORY_AND_DISK_SER)

  // La terminologie que l'on trouve ici n'est pas typique Spark mais plutôt Scala :

  /* Attention : l'implémentation suivante est dangeureuse !
    => Voir doc dans le code de Spark pour la méthode groupByKey...
    => Pour cela, on va préférer faire ici plutôt un aggregate

  cleanDataset.groupByKey()
    .mapValues(_.max)
    .foreach(println)

    On utilise plutôt aggregateByKey :
  */

  // Calcul de le max des températures par horaires :
  cleanDataset.aggregateByKey(Float.NegativeInfinity)((m1:Float, m2:Float) => Math.max(m1, m2), (m1:Float, m2:Float) => Math.max(m1, m2))
    .saveAsTextFile("/tmp/max-temp")

  // Calcul de la moyenne des températures par horaires :
  cleanDataset.map((kv:(String, Float)) => (kv._1, (kv._2, 1)))
      .reduceByKey((kv1:(Float, Int), kv2:(Float, Int)) => (kv1._1 + kv2._1, kv1._2+ kv2._2))
      .mapValues(sc => sc._1/sc._2)
      .saveAsTextFile("/tmp/mean-temp")

  Thread.sleep(10000)

  sc.stop()

}

case class RawDatapoint(time:String, measure:String)
case class Datapoint(time:String, measure:Float)
