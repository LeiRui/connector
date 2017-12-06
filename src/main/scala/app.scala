import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by rana on 29/9/16.
  */
object app extends App {

  /**
    * see https://stackoverflow.com/questions/24832284/nullpointerexception-in-spark-sql
    * System.setProperty("hadoop.home.dir", "c:\\winutils\\") to fix windows problem
    */
  System.setProperty("hadoop.home.dir", "c:\\winutils\\")

  println("Application started...")

  /**
    * file:///... to fix /again\warehouse problem
    */
  var spark=SparkSession.builder().master("local").appName("spark-custom-datasource")
    .config("spark.sql.warehouse.dir","file:///E:/code/Intellij IDEA/again/spark-warehouse")
    .getOrCreate()

  //val df = spark.sqlContext.read.format("external").load("data/input1.txt")
  val df = spark.sqlContext.read.format("external").load("examples/customs1/part-00000")

  //print the schema
  df.printSchema()

  //print the data
  df.show()

  //save the data
  ////df.write.options(Map("format" -> "external")).mode(SaveMode.Overwrite).save("examples/customs/")
  //df.write.format("external").mode(SaveMode.Overwrite).save("examples/customs1/")
  //df.write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save("examples/csv/1.csv")
  //df.select("name").write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save("examples/csv/1_name.csv")


  //filter data
  df.createOrReplaceTempView("test")
  spark.sql("select * from test where salary = 30000").show()

  //select some specific columns
  //  df.createOrReplaceTempView("test")
 // spark.sql("select id, name, salary from test").show()


  //df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("external").save("out_custom/")
  //df.write.options(Map("format" -> "csv")).mode(SaveMode.Overwrite).format("external").save("out_csv/1.csv")
  //df.write.mode(SaveMode.Overwrite).format("external").save("out_none/")

  println("Application Ended...")
}