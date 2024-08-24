import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object wc4 extends App {

  //Logger.getLogger("org").setLevel(Level.ERROR)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "RDD-Example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
//mapping fields using scala functions
  /*def parseline(x: String) = {
    val fields = x.split(",")
    val id = fields(0)
    val discount = fields(5).toFloat
    (id, discount)
  }*/
  val RDD1 = spark.sparkContext.textFile("F:/Dataset/order_items_new.csv")

  //take two colums order_id,discount
  val RDD2 = RDD1.map(x=>(x.split(",")(0).toInt,x.split(",")(5).toFloat))
  //val RDD2 =RDD1.map(parseline)

  //RDD2.take(5).foreach(println)
  val RDD3 =RDD2.reduceByKey((x,y) => x+y).sortBy(x => x._1)
  println("Actions===>foreach")
    RDD3.collect.foreach(println)
  println("Actions===>count")
  println(RDD3.count())
  println("Actions===>countbyvalue")
  RDD3.countByValue().foreach(println)
  println("Actions===>min")
  println(RDD3.min())
  println("Actions===>max")
  println(RDD3.max())

  spark.stop()
}
/*It is an action
  It returns the count of each unique value in an RDD
  as a local Map (as a Map to driver program) (value, countofvalues) pair
  Care must be taken to use this API since it returns the value to
   driver program so it’s suitable only for small values.*/