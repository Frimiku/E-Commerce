package warehouse

import core.SparkFactory
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StringType

object ECAdsFR {
  val logger = Logger.getLogger(ECAdsFR.getClass)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkFactory()
      .build()
      .baseConfig("ec_ads")
      .optimizeDriver()
      .optimizeExecutor()
      .optimizeLimit(maxLocalWaitS = 100)
      .optimizeSerializer()
      .optimizeNetAbout()
      .optimizeDynamicAllocation(true)
      .optimizeShuffle()
      .optimizeSpeculation(true)
      .optimizeRuntime(adaptiveEnabled = true, cboEnabled = true)
      .warehouseDir("hdfs://single:9000/hive312/warehouse")
      .end()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    /**
     * 创建应用层仓库(hive)：ebs_ads_report
     */
    spark.sql("create database if not exists ec_ads_report")

    // 维度数据 => dwd
    val frmCustomer: DataFrame = spark.table("ec_dwd.customer").cache()
    val frmStore: DataFrame = spark.table("ec_dwd.store").cache()
    // 将广播变量（维度数据）广播出去
    val bcCustomer: Broadcast[DataFrame] = spark.sparkContext.broadcast(frmCustomer)
    val bcStore: Broadcast[DataFrame] = spark.sparkContext.broadcast(frmStore)

    /**
     * 客户分析
     */
    // 大屏四处的显示数据：一行四列（总人数，中国客户数，美国客户数，加拿大客户数）【按照列绑定数据】
    spark.table("ec_dwd.customer")
      .groupBy("country")
      .agg(count("*").as("country_cnt"))
      // 行转列(=> 一行四列)
      .agg(
        sum("country_cnt").as("all_cnt"),
        sum(when($"country" === "China", $"country_cnt").otherwise(0)).as("china_cnt"),
        sum(when($"country" === "United States", $"country_cnt").otherwise(0)).as("usa_cnt"),
        sum(when($"country" === "Canada", $"country_cnt").otherwise(0)).as("canada_cnt")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.customer_singles")

    // 销售额占比
    spark.table("ec_dws.transaction_by_country")
      .select("country", "sum_amount")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.distribution_by_sum_amount")

    // 销售最多的前10名客户
    spark.table("ec_dws.transaction_by_ymc")
      .where($"gid" === 6)
      .select("customer_id", "sum_amount")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"sum_amount".desc)))
      .where($"rnk" <= 10)
      .join(bcCustomer.value, "customer_id") // 用户名
      .select("customer_name", "sum_amount", "rnk")
      .orderBy($"rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.customer_top10_by_sum_amount")

    // 客户常用信用卡
    spark.table("ec_dws.customer_by_cgc")
      .where($"gid" === 6)
      .withColumn("rnk", dense_rank().over(Window.orderBy($"agg_cnt".desc)))
      .select("credit_type", "rnk")
      .orderBy("rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.credit_type")

    // 季度独立客户数
    spark.table("ec_dws.uq_customer_count_by_qw")
      .where($"gid" === 3)
      .orderBy()
      .select(concat_ws("_",
        $"tran_year".cast(StringType),
        $"tran_quarter".cast(StringType)
      ).as("year_quarter"),
        $"uq_customer_count"
      )
      .orderBy("year_quarter")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.uq_customer_cnt_by_year_quarter")

    // 热点工作岗位(Top5)
    spark.table("ec_dws.customer_by_job")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"job_cnt".desc)))
      .where($"rnk" <= 5)
      .orderBy("rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.job_top5")

    // 销售额前10的客户占比
    // 找出【交易量】最大的10个【客户】(【前十】)
    spark.table("ec_dws.transaction_by_ymc")
      .where($"gid" === 6)
      .select("customer_id", "tran_count")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"tran_count".desc)))
      .where($"rnk" <= 10)
      .join(bcCustomer.value, "customer_id") // 用户名
      .select("customer_name", "tran_count", "rnk")
      .orderBy($"rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.customer_top10_by_tran_count")

    /**
     * 店铺统计
     */
    // 店铺交易情况
    // 交易情况前三名: Lablaws,Walmart,FoodMart
    spark.table("ec_dws.store_tran_factor")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"sale_factor".desc)))
      .where($"rnk" <= 3)
      .join(bcStore.value, "store_id")
      .select("store_name", "sale_factor", "rnk")
      // 大屏三处展示数据【按照列绑定数据】
      .agg(
        sum(when($"store_name" === "Lablaws", $"sale_factor").otherwise("0")).as("lablaws_sale_factor"),
        sum(when($"store_name" === "Walmart", $"sale_factor").otherwise("0")).as("walmart_sale_factor"),
        sum(when($"store_name" === "FoodMart", $"sale_factor").otherwise("0")).as("foodmart_sale_factor"),
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.store_singles")

    // 店铺评价5分分布
    spark.table("ec_dws.review_distribution_of_store")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.review5_distribution_store")

    // 评分分布
    spark.table("ec_dws.review_distribution_of_store")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.review_distribution_of_store")

    // 月均销量热点商品
    spark.table("ec_dws.transaction_product")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"avg_month_count".desc))) // 总价
      .where($"rnk" <= 5)
      .select("product", "avg_month_count", "rnk")
      .orderBy("rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.product_top5_by_avg_month_sale_cnt")

    // 店铺热点商品排名
    // 根据客户数量找出最受欢迎的5种产品
    spark.table("ec_dws.transaction_product")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"uq_customer_count".desc))) // 总价
      .where($"rnk" <= 5)
      .select("product", "uq_customer_count", "rnk")
      .orderBy("rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.rank_of_hot_product")

    // 按年(只有2018数据)和月计算每家店的收入的环比(关联维度数据，保证数据完整型)
    val frmStoreYearMonth: DataFrame = spark
      .table("ec_dws.store_year_month_sum_amount")
      .cache()
    spark.table("ec_dws.dim_date")
      .where($"tran_year" === 2018) // 锁定为2018，否则以目前的数据而言，会出错的
      .groupBy("tran_year", "tran_month")
      .count() // 到此处，为了防止数据有缺失
      .crossJoin(bcStore.value)
      .join(frmStoreYearMonth, Seq("store_id", "tran_year", "tran_month"), "left")
      // 环比
      .withColumn("lag_sum_amount",
        lag("sum_amount", 1).over(Window.partitionBy("store_id").orderBy("tran_month"))
      )
      .where($"tran_month" =!= 1)
      .select($"store_name",
        concat($"tran_month".cast(StringType), lit("/"), ($"tran_month" - 1).cast(StringType)).as("chain_name"),
        ($"sum_amount" - $"lag_sum_amount").as("chain_value")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.store_2018_month_chain")

    /**
     * 交易情况
     */
    // 各类交易汇总（国内外销售情况+销售总额+销售总数+男女占比）
    // 大屏八处数据展示：交易情况(国内国外对比)【按照列绑定数据】
    // 国内外总额，总销量，总客户数
    // 销售总额，总销量
    spark.table("ec_dws.transaction_by_country")
      // 国内外
      .withColumn("country", when($"country" === "China", "domestic").otherwise("foreign"))
      .groupBy("country")
      .agg(
        sum("sum_amount").as("sum_amount"), // 总额
        sum("tran_count").as("tran_count"), // 销量
        sum("uq_customer_count").as("uq_customer_count") // 唯一客户数
      )
      .agg(
        sum("sum_amount").as("all_amount"),
        sum("tran_count").as("all_count"),
        sum(when($"country" === "domestic", $"sum_amount").otherwise(0)).as("domestic_amount"), //国内总额
        sum(when($"country" === "foreign", $"sum_amount").otherwise(0)).as("foreign_amount"), //国外总额
        sum(when($"country" === "domestic", $"tran_count").otherwise(0)).as("domestic_count"), //国内总量
        sum(when($"country" === "foreign", $"tran_count").otherwise(0)).as("foreign_count"), //国外总量
        sum(when($"country" === "domestic", $"uq_customer_count").otherwise(0)).as("domestic_uq_customer_count"), //国内客户数
        sum(when($"country" === "foreign", $"uq_customer_count").otherwise(0)).as("foreign_uq_customer_count") //国外客户数
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.transaction_singles_one")
    // 男客户数，女客户数
    spark.table("ec_dws.customer_by_cgc")
      .where($"gid" === 1)
      .groupBy("gender")
      .agg(
        sum("agg_cnt").as("customer_count")
      )
      .agg(
        sum(when($"gender" === "Male", $"customer_count").otherwise(0)).as("male_customer_count"),
        sum(when($"gender" === "Female", $"customer_count").otherwise(0)).as("female_customer_count"),
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.transaction_singles_two")

    // 国家销售总额世界占比
    spark.table("ec_dws.transaction_by_country")
      .select("country", "sum_amount")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.country_distribution_by_sum_amount")

    // 商品销售额Top5
    // 按总价找出最受欢迎的5种产品
    spark.table("ec_dws.transaction_product")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"sum_amount".desc))) // 总价
      .where($"rnk" <= 5)
      .select("product", "sum_amount", "rnk")
      .orderBy("rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.product_top5_by_sum_amount")

    // 时段交易额
    // 按时间段计算总收入
    spark.table("ec_dws.transaction_by_dim_date")
      .where($"gid" === 0)
      .groupBy("tran_range")
      .agg(sum("sum_amount").as("sum_amount"))
      .orderBy("tran_range")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.sum_amount_by_range")

    // 热销产品
    spark.table("ec_dws.transaction_product")
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ec_ads_report.tran_distribution_by_product")

    // 季度销售额
    // 每个季度的总收入
    spark.table("ec_dws.transaction_by_dim_date")
      .where($"gid" === 15)
      .select("tran_year", "tran_quarter", "sum_amount")
      .orderBy("tran_year", "tran_quarter")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.sum_amount_by_year_quarter")

    // 商品销售额前三
    // 按总价找出最受欢迎的3种产品
    spark.table("ec_dws.transaction_product")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"sum_amount".desc))) // 总价
      .where($"rnk" <= 3)
      .select("product", "sum_amount", "rnk")
      .orderBy("rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.product_top3_by_sum_amount")

    // 产品独立客户数
    // 根据客户数量找出最受欢迎的5种产品
    spark.table("ec_dws.transaction_product")
      .withColumn("rnk", dense_rank().over(Window.orderBy($"uq_customer_count".desc))) // 总价
      .where($"rnk" <= 5)
      .select("product", "uq_customer_count", "rnk")
      .orderBy("rnk")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_ads_report.product_top5_by_uq_customer_cnt")

    spark.stop()
    logger.info("EBS ADS FINISHED")
  }
}
