import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext

class Top10ProductsInCategoriesSQL {
  def run(events: DataFrame, sqlContext: HiveContext, url: String,prop: java.util.Properties): Unit = {

    sqlContext
      .sql(
        "SELECT rs.category, rs.product, rs.cnt " +
          "FROM (" +
          "SELECT " +
          "ROW_NUMBER() OVER (PARTITION BY category ORDER BY cnt DESC) AS r, " +
          "tt.category AS category, " +
          "tt.product AS product, " +
          "tt.cnt AS cnt " +
          "FROM (" +
          "SELECT events.category, events.product, COUNT(*) AS cnt " +
          "FROM events as events " +
          "GROUP BY events.category, events.product " +
          "ORDER BY events.category, cnt DESC) AS tt) rs " +
          "WHERE rs.r <= 10"
      )
      .write.mode(SaveMode.Overwrite).jdbc(url, "top10ProductsInCategoriesSQL", prop)


  }
}

