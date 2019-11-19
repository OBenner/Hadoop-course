
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

class Top10CountriesByMoneySpendingSQL {


  def run(events: DataFrame, locationsDf: DataFrame,blocksDf: DataFrame, sqlContext: HiveContext, url: String,prop: java.util.Properties): Unit = {

    blocksDf.registerTempTable("blocksDf")
    locationsDf.registerTempTable("locationsDf")
    locationsDf.cache()
    blocksDf.cache()
    events.cache()


    sqlContext.udf.register("is_in_range",
      (ip: String, network: String) =>  IpMatcher.`match`(network,ip))

    val topCountries = sqlContext.sql(
      "select country_name, sum(price) as sum from " +
        "blocksDf inf join " +
        "locationsDf ipp on inf.geoname_id = ipp.geoname_id " +
        "join events e  where   is_in_range(e.ip,network) " +
        "group by country_name  having country_name is not NULL and " +
        "country_name != ''  order by sum desc limit 10") .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "top10CountriesByMoneySpendingSQL", prop)

  }

}
