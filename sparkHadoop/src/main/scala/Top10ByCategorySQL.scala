import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext

class Top10ByCategorySQL {
  def run(events: DataFrame, sqlContext: HiveContext, url: String,prop: java.util.Properties): Unit = {

    sqlContext.sql("select category, count(*) as sum from events group by category order by sum desc limit 10")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "top10ByCategorySQL", prop)

  }
}
