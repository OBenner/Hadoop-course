import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class Top10ProductsInCategoriesRDD extends java.io.Serializable {
  def run(events: DataFrame, sContext: SparkContext, sqlContext: HiveContext, url: String, prop: java.util.Properties): Unit = {

    val result52 = events.rdd.map(p => ((p(3), p(0)), 1))
      .aggregateByKey(0)((acc, v) => acc + v, (acc1, acc2) => acc1 + acc2)
      .groupBy(_._1._1)
      .mapValues(l =>
        l.toList.sortWith(_._2 > _._2)
          .map(e => (e._1._2, e._2))
          .take(10))
      .flatMapValues(x => x)
      .map { case (c, (n, o)) => (c.toString, n.toString, o.toInt) }


    val schema = StructType(
      List(
        StructField("category", DataTypes.StringType),
        StructField("product", DataTypes.StringType),
        StructField("num_of_transactions", DataTypes.IntegerType)
      )
    )
    sqlContext.createDataFrame(result52)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "top10ProductsInCategoriesRDD", prop)

  }


}
