

/*

https://stackoverflow.com/questions/43533751/spark-reading-from-postgres-jdbc-table-slow

*/

// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
// import java.sql._;
import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.mutable.ArrayBuffer;
 
// define main method (Spark entry point)
object HelloWorld {

  // def exec() {

  //   // val dbHost = ""

  //   val url = ("jdbc:postgresql://localhost/postgres"+ 
  //   "?tcpKeepAlive=true&prepareThreshold=-1&binaryTransfer=true&defaultRowFetchSize=10000")

  //   val conn = DriverManager.getConnection(url, "postgres", "root");

  //   val sqlText = """SELECT "cityid", "cname", "userId", "state" 
  //           FROM city 
  //           """

  //   val t0 = System.nanoTime()

  //   val stmt = conn.prepareStatement(sqlText, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

  //   val rs = stmt.executeQuery()

  //   val list = new ArrayBuffer[(Long, Long, Long, Double)]()

  //   while (rs.next()) {
  //       val seriesId = rs.getLong("seriesId")
  //       val companyId = rs.getLong("companyId")
  //       val userId = rs.getLong("userId")
  //       val score = rs.getDouble("score")
  //       list.append((seriesId, companyId, userId, score))
  //   }

  //   val t1 = System.nanoTime()

  //   println("Elapsed time: " + (t1 - t0) * 1e-9 + "s")

  //   println(list)

  //   println(list.size)

  //   rs.close()
  //   stmt.close()
  //   conn.close()
  // }

  def exec2(spark: SparkSession) {
    
    val url = ("jdbc:postgresql://localhost/postgres"+ 
    "?tcpKeepAlive=true&prepareThreshold=-1&binaryTransfer=true&defaultRowFetchSize=10000")

    val driver = "org.postgresql.Driver"

    Class.forName(driver)

    val t0 = System.nanoTime()

    val sqlText = """SELECT "cityid", "cname", "userId", "state" 
            FROM city 
            """

    val df = spark.read
          .format("jdbc")
          .option("url", url)
          .option("dbtable", sqlText)
          .option("user", "postgres")
          .option("password", "root")
          .load()

    val list = df.collect()

    val t1 = System.nanoTime()

    println("Elapsed time: " + (t1 - t0) * 1e-9 + "s")

    print (list.size)
  }
  
  def main(args: Array[String]) {
 
    // initialise spark context
    // val conf = new SparkConf().setAppName(HelloWorld.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config("spark.jars", "/Users/str-kwml0020/projects/postgresql-42.3.0.jar").getOrCreate()

    // do stuff
    println("************")
    println("************")
    println("Hello, world!")
    
    val rdd = spark.sparkContext.parallelize(Array(1 to 10))
    // println(rdd.count())
    
    exec2(spark)

    println("************")
    println("************")
    
    // terminate spark context
    spark.stop()
    
  }
}

