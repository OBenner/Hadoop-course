
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class Top10CountriesByMoneySpendingRDD {

  def run(eventss: DataFrame, locationsDf: DataFrame, blocksDf: DataFrame, sContext: SparkContext, sqlContext: HiveContext, url: String, prop: java.util.Properties): Unit = {

    val blockRdd = sContext.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ips")
    val blockHeader = blockRdd.first
    val rectifiedBlockRdd = blockRdd
      .filter(_ != blockHeader)
      .map(_.split(","))

    val locationRdd = sContext.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/ipc")
    val locationHeader = locationRdd.first
    val rectifiedLocationRdd = locationRdd
      .filter(_ != locationHeader)
      .map(_.split(","))


    val loc = rectifiedLocationRdd
      .filter(l => !l(0).isEmpty && !l(5).isEmpty)
      .map(arr =>
        (arr(0), arr))


    val doc = rectifiedBlockRdd
      .filter(l => !l(0).isEmpty && !l(1).isEmpty)
      .map(arr => (arr(1), arr))

    val ipj = loc.join(doc)
      .map(row => (row._2._1(5), row._2._2(0).mkString("")))
      .keyBy(_._2)
      .mapValues(_._1)
      .groupByKey()
      .map{
        case (ip, country) => (ip.toString, country.toString())
      }

    ipj.cache()
    sContext.broadcast(ipj)
    ipj.foreach(event => println(event))


    val ipPriceRdd = eventss
      .map(arr => (arr(4).toString, arr.getDouble(1)))
    ipPriceRdd.cache()

    val best10Countries = ipPriceRdd
      .cartesian(ipj)
      .collect {
        case ((ip, price), (net, country))
          if !ip.isEmpty && !net.isEmpty && IpMatcher.`match`(net.toString, ip.toString)
        => (country, price)
      }.
      reduceByKey(_ + _).
      sortBy(_._2, false, 1).
      map(x => Row(x._1, x._2)).
      take(10)

    val schema = StructType(
      List(
        StructField("country", DataTypes.StringType),
        StructField("price", DataTypes.DoubleType)
      )
    )

    sqlContext
      .createDataFrame(sContext.parallelize(best10Countries), schema)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "top10CountriesByMoneySpendingRDD", prop)
  }

}
