// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
 
// define main method (Spark entry point)
object HelloWorld {
  def main(args: Array[String]) {
 
    // initialise spark context
    val conf = new SparkConf().setAppName(HelloWorld.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    val rdd = spark.sparkContext.parallelize(Array(1 to 10))
    println(rdd.count())
    println("************")
    println("************")
    
    // terminate spark context
    spark.stop()
    
  }
}


var i=0
var length=0
val data=Source.fromFile(/home/nagulraj/tact/datasets/user-2022-01-05.csv)
for (line <- data.getLines) {
  val cols = line.split(",").map(_.trim)
  length = cols.length  
  while(i<length){
    println(cols(i))
    i=i+1
  }
  i=0
}

