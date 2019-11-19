
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{BooleanType,  DoubleType, IntegerType, StringType, StructField, StructType}

object App {
  def main(args: Array[String]): Unit = {
    val sConf = new SparkConf()
      .setAppName("sparkhdoop")
      .setMaster("local[*]");


    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "cloudera")
    val url = "jdbc:mysql://localhost:3306/sparkdb"
    val sContext = new SparkContext(sConf)
    val sqlContext = new HiveContext(sContext)

    val eventsDF = {
      val eventsSchema = StructType(Array(
        StructField("product", StringType),
        StructField("price", DoubleType),
        StructField("date", StringType),
        StructField("category", StringType),
        StructField("ip", StringType)
      ))
      sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("delimiter", "\t") // req
        .schema(eventsSchema).load("hdfs://quickstart.cloudera:8020/user/cloudera/events/2019/*/*")
    }


    val blocksDf = {
      val schema = StructType(Array(
        StructField("network", StringType, true),
        StructField("geoname_id", IntegerType, true),
        StructField("registered_country_geoname_id", IntegerType, true),
        StructField("represented_country_geoname_id", IntegerType, true),
        StructField("is_anonymous_proxy", BooleanType, true),
        StructField("is_satellite_provider", BooleanType, true)
      ))

      sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",") // req
        .schema(schema)
        .load("hdfs://quickstart.cloudera:8020/user/cloudera/ips")
    }

    val locationsDf = {
      val schema = StructType(Array(
        StructField("geoname_id", IntegerType, true),
        StructField("locale_code", StringType, true),
        StructField("continent_code", StringType, true),
        StructField("continent_name", StringType, true),
        StructField("country_iso_code", StringType, true),
        StructField("country_name", StringType, true),
        StructField("is_in_european_union", BooleanType, true)
      ))

      sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",") // req
        .schema(schema)
        .load("hdfs://quickstart.cloudera:8020/user/cloudera/ipc")
    }

    eventsDF.registerTempTable("events")
    blocksDf.registerTempTable("blocksDf")
    locationsDf.registerTempTable("locationsDf")

    args(0) match {
      case arg if arg.contains("t1sql") =>
        val t1sql = new Top10ByCategorySQL()
        t1sql.run(eventsDF, sqlContext, url, prop)

      case arg if arg.contains("t2sql") =>
        val t2sql = new Top10ProductsInCategoriesSQL()
        t2sql.run(eventsDF, sqlContext, url, prop)

      case arg if arg.contains("t3sql") =>
        val t3sql = new Top10CountriesByMoneySpendingSQL()
        t3sql.run(eventsDF, locationsDf, blocksDf, sqlContext, url, prop)

      case arg if arg.contains("t1rdd") =>
        val t1rdd = new Top10ByCategoryRDD()
        t1rdd.run(eventsDF, sqlContext, url, prop)

      case arg if arg.contains("t2rdd") =>
        val t2rdd = new Top10ProductsInCategoriesRDD()
        t2rdd.run(eventsDF, sContext, sqlContext, url, prop)

      case arg if arg.contains("t3rdd") =>
        val t3rdd = new Top10CountriesByMoneySpendingRDD()
        t3rdd.run(eventsDF, locationsDf, blocksDf, sContext, sqlContext, url, prop)

      case _ =>

        throw new RuntimeException("choose exercise")
    }


  }
}

