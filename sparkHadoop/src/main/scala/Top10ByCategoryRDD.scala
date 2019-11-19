
import org.apache.spark.sql.{DataFrame,  SaveMode}
import org.apache.spark.sql.hive.HiveContext
class Top10ByCategoryRDD {

  def run(events: DataFrame, sqlContext: HiveContext, url: String,prop: java.util.Properties): Unit = {

    val categoryCounted = events
      .rdd
      .map(r => (r.getString(3), 1))
      .reduceByKey(_ + _)

    val top10ByCategory = categoryCounted
      .top(10)(Ordering.by(_._2))
      .sortBy(_._2)(Ordering.Int.reverse)

  sqlContext.createDataFrame(top10ByCategory)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "top10ByCategoryRDD", prop)

  }
}
