package warehouse

import core.SparkFactory
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, date_add, max, min, to_date}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.Date

object ECDws {
  val logger = Logger.getLogger(ECDws.getClass)
  case class Review(review_score:Float)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkFactory()
      .build()
      .baseConfig("ec_dws")
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
     * 创建数仓轻聚层库(hive)：ec_des
     */
    spark.sql("create database if not exists ec_dws")

    // 日期维度表
    import core.Validator.dimDate
    val rowDt: Row = spark.table("ec_dwd.transaction")
      .agg(
        date_add(max(to_date($"tran_dt")), 1).as("max_date"),
        min(to_date($"tran_dt")).as("min_date")
      )
      .take(1)(0)
    val minDate: String = rowDt.getAs[Date]("min_date").toString
    val maxDate: String = rowDt.getAs[Date]("max_date").toString
    spark
      .createDataFrame(dimDate(minDate, maxDate))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.dim_date")

    /**
     * 客户表 customer
     */
    // 客户表(国家、性别、信用卡类型)
    /*
       credit_type 					        6
       gender,country					      1
       country,gender,credit_type		0
     */
    spark.sql(
        """
          |select
          | grouping__id as gid,country,gender,credit_type,count(customer_id) as agg_cnt
          |from ec_dwd.customer
          |group by country,gender,credit_type
          |grouping sets(credit_type,(gender,country),(country,gender,credit_type))
          |""".stripMargin)
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .partitionBy("gid")
      .saveAsTable("ec_dws.customer_by_cgc")

    // 客户表(工种)
    spark
      .table("ec_dwd.customer")
      .groupBy("job")
      .agg(count("customer_id").as("job_cnt"))
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ec_dws.customer_by_job")

    // 客户表(邮箱)
    spark
      .table("ec_dwd.customer")
      .groupBy("email")
      .agg(count("customer_id").as("email_cnt"))
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ec_dws.customer_by_email")

    /**
     * 交易表 transaction
     */
    // 交易表(日期)
    /*
     * 字典：
     *  0   "tran_year","tran_quarter","tran_month","tran_month_week","tran_day","tran_range"
     *  1   "tran_year","tran_quarter","tran_month","tran_month_week","tran_day"
     *  3   "tran_year","tran_quarter","tran_month","tran_month_week"
     *  7   "tran_year","tran_quarter","tran_month"
     *  15  "tran_year","tran_quarter"
     *  31  "tran_year"
     *  63
     */
    val date: DataFrame = spark.table("ec_dws.dim_date")
    spark
      .table("ec_dwd.transaction")
      .as("tran")
      .join(date.as("dim"),
        Seq("tran_year", "tran_quarter", "tran_month", "tran_month_week", "tran_day", "tran_range"),
        "right"
      )
      .rollup("tran_year", "tran_quarter", "tran_month", "tran_month_week", "tran_day", "tran_range")
      .agg(
        grouping_id().as("gid"),
        sum($"price").cast(DecimalType(10, 2)).as("sum_amount"),
        avg($"price").cast(DecimalType(10, 2)).as("avg_amount"),
        count($"price").as("tran_count") // 交易数
      )
      // 再次清洗：去除null,用withColumn覆盖
      .withColumn("sum_amount",
        when($"sum_amount".isNull, 0).otherwise($"sum_amount")
      )
      .withColumn("avg_amount",
        when($"avg_amount".isNull, 0).otherwise($"avg_amount")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.transaction_by_dim_date")

    // 交易表(顾客)
    /*
     交易、客户、日期客户
     dim_year,dim_month,customer_id		0
     customer_id 					            6
    */
    spark.sql(
        """
          |with dim_year_month as(
          | select
          |   tran_year,tran_month
          | from ec_dws.dim_date
          | group by tran_year,tran_month
          |),
          |dim_year_month_customer as(
          | select
          |   D.tran_year,D.tran_month,C.customer_id
          | from ec_dwd.customer as C
          | cross join dim_year_month as D
          |)
          |select
          |   grouping__id as gid,
          |   tran_year,tran_month,customer_id,
          |   cast(sum(price) as decimal(10,2)) as sum_amount,
          |   cast(avg(price) as decimal(10,2)) as avg_amount,
          |   count(price) as tran_count
          |from ec_dwd.transaction
          |right join dim_year_month_customer
          |using(tran_year,tran_month,customer_id)
          |group by tran_year,tran_month,customer_id
          |grouping sets(customer_id,(tran_year,tran_month,customer_id))
          |""".stripMargin)
      // 再次清洗：去除null,用withColumn覆盖
      .withColumn("sum_amount",
        when($"sum_amount".isNull, 0).otherwise($"sum_amount")
      )
      .withColumn("avg_amount",
        when($"avg_amount".isNull, 0).otherwise($"avg_amount")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .partitionBy("tran_year", "tran_month")
      .saveAsTable("ec_dws.transaction_by_ymc")

    // 交易表(产品)
    val avgMonthCount: DataFrame = spark.table("ec_dwd.transaction")
      .groupBy("product", "tran_month")
      .agg(count("transaction_id").as("month_count"))
      .groupBy("product")
      .agg(avg("month_count").as("avg_month_count"))
    spark.table("ec_dwd.transaction")
      .groupBy("product")
      .agg(
        sum("price").cast(DecimalType(10, 2)).as("sum_amount"),
        size(collect_set("customer_id")).as("uq_customer_count"), // 客户总数
        count("transaction_id").as("tran_count") // 订单数
      )
      .join(avgMonthCount, Seq("product"), "inner")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.transaction_product")

    // 交易表(国家)
    val customer: DataFrame = spark
      .table("ec_dwd.customer")
    // 广播
    val bcDF: Broadcast[DataFrame] = spark.sparkContext.broadcast(customer)
    spark
      .table("ec_dwd.transaction").as("T")
      // 获取广播出去的小表
      .join(bcDF.value.as("C"), Seq("customer_id"), "inner")
      .groupBy("C.country")
      .agg(
        sum("T.price").cast(DecimalType(10, 2)).as("sum_amount"),
        avg("T.price").cast(DecimalType(10, 2)).as("avg_amount"),
        count($"T.customer_id").as("tran_count"), // 交易数
        size(collect_set("customer_id")).as("uq_customer_count") // 唯一客户数
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.transaction_by_country")

    // 独立用户(季度&周)
    /*
     字典：
       0：所有数据【周独立用户数】
       3：季度独立用户数
    */
    spark.table("ec_dwd.transaction")
      .rollup("tran_year", "tran_quarter", "tran_month", "tran_month_week")
      .agg(
        grouping_id().as("gid"),
        size(collect_set("customer_id")).as("uq_customer_count")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.uq_customer_count_by_qw")

    /**
     * 店铺 store
     */
    // 交易表(店铺)
    spark.table("ec_dwd.transaction")
      .groupBy("store_id")
      .agg(
        count("transaction_id").as("customer_count"),
        sum("price").as("sum_amount")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.transaction_by_store")

    // 交易表(店铺交易情况)
    /*
      店铺交易情况 = 交易额+独立客户数+年周销售额TOP3次数+年周平均独立客户数+月均新增独立客户数+均评分
      一般思想：归一化 => 先求出各个条件中的最大值，然后套入公式：(每列值/列中最大值)*权重
     */
    // 1、年周平均独立客户数
    val weekAvg: DataFrame = spark.table("ec_dwd.transaction")
      .groupBy("store_id", "tran_year", "tran_month", "tran_month_week")
      .agg(
        size(collect_set("customer_id")).as("week_uq_customer_cnt") // 周进行汇总
      )
      .groupBy("store_id") // 均值
      .agg(avg("week_uq_customer_cnt").as("week_avg_uq_counter_cnt"))
    // 2、月均新增购买的独立客户数【一般思路：新增用户=>差集】
    // 此处另一种思路
    val monthNewAdd: DataFrame = spark.table("ec_dwd.transaction")
      // 最早购买并排序
      .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy("tran_dt")))
      .where($"rn" === 1) // 选取第一条数据（用户第一次购买）
      .groupBy("store_id", "tran_month")
      .agg(count("customer_id").as("month_add_customer_cnt"))
      .groupBy("store_id")
      .agg(avg("month_add_customer_cnt").as("avg_month_add_customer_cnt"))
    // 3、均评分
    val avgReview: DataFrame = spark
      .table("ec_dwd.review")
      .groupBy("store_id")
      .agg(avg("review_score").as("avg_review_score"))
    // 4、年周销售额TOP3次数
    val weekTop3: DataFrame = spark.table("ec_dwd.transaction")
      .groupBy("store_id", "tran_year", "tran_month", "tran_month_week")
      .agg(sum("price").as("sum_amount"))
      // 先开窗口进行排名，再筛选前三
      .withColumn("week_rnk",
        dense_rank()
          .over(Window.partitionBy("tran_year", "tran_month", "tran_month_week")
            .orderBy($"sum_amount".desc)
          ))
      .where($"week_rnk" <= 3)
      .groupBy("store_id")
      .agg(count("week_rnk").as("week_top3_count"))
    // 汇总：六个指标的汇总
    val all = spark.table("ec_dwd.transaction")
      .groupBy("store_id")
      .agg(
        sum("price").as("sum_amount"), // 交易额
        size(collect_set("customer_id")).as("total_uq_customer_cnt"), // 总的独立客户数（一共有多少买过东西的客户）
      )
      .join(weekAvg, "store_id")
      .join(monthNewAdd, "store_id")
      .join(avgReview, "store_id")
      .join(weekTop3, "store_id")
      .cache()
    // 归一化操作
    val allMax = all
      .agg(
        // 先求出六个指标中的最大值
        max("sum_amount").as("max_sum_amount"),
        max("total_uq_customer_cnt").as("max_uq_cust_cnt"),
        max("week_avg_uq_counter_cnt").as("max_week_avg_uq_cust_cnt"),
        max("avg_month_add_customer_cnt").as("max_month_add_cust_cnt"),
        max("avg_review_score").as("max_avg_review_score"),
        max("week_top3_count").as("max_week_top3_cnt"),
      )
    // 综合指标【1以内小数】
    // (每个值/该列最大值)*权重【权重自己定】
    all.as("A").crossJoin(allMax.as("M"))
      .select($"store_id",
        (
          (($"A.sum_amount" / $"M.max_sum_amount") * 0.1).cast(DecimalType(5, 4)) +
            (($"A.total_uq_customer_cnt" / $"M.max_uq_cust_cnt") * 0.1).cast(DecimalType(5, 4)) +
            (($"A.week_top3_count" / $"M.max_week_top3_cnt") * 0.3).cast(DecimalType(5, 4)) +
            (($"A.week_avg_uq_counter_cnt" / $"M.max_week_avg_uq_cust_cnt") * 0.1).cast(DecimalType(5, 4)) +
            (($"A.avg_month_add_customer_cnt" / $"M.max_month_add_cust_cnt") * 0.3).cast(DecimalType(5, 4)) +
            (($"A.avg_review_score" / $"M.max_avg_review_score") * 0.1).cast(DecimalType(5, 4))
          ).as("sale_factor") // 店铺销售质量指数
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      // 店铺交易综合指标
      .saveAsTable("ec_dws.store_tran_factor")

    // 交易表(店铺产品，唯一客户数量)
    spark.table("ec_dwd.transaction")
      .groupBy("store_id", "product")
      .agg(size(collect_set("customer_id")).as("uq_customer_cnt"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.pop_product_of_store")

    // 交易表(店铺员工顾客比)
    val storeUqCust: DataFrame = spark.table("ec_dwd.transaction")
      .groupBy("store_id")
      .agg(size(collect_set("customer_id")).as("uq_customer_cnt"))
    spark
      .table("ebs_dwd.store")
      .join(storeUqCust, "store_id")
      .select($"store_id", ($"employee_number" / $"uq_customer_cnt").as("emp_cus_ratio"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.store_emp_cus_ration")

    // 交易(店铺年月销售额)
    spark.table("ec_dwd.transaction")
      .groupBy("store_id", "tran_year", "tran_month")
      .agg(sum("price").as("sum_amount"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.store_year_month_sum_amount")

    // 交易(店铺时段流量)
    spark.table("ec_dwd.transaction")
      .groupBy("store_id", "tran_range")
      .agg(count("transaction_id").as("range_flow"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.store_range_flow")

    // 交易(店铺忠实粉丝)
    /* 店铺的忠实粉丝：
        标准：花钱额度，购买次数，月均购买次数，近3个月的额度，近3个月的次数，近3周的额度，近3周的次数
        归一化 => 先求出各个条件中的最大值，然后套入公式：(每列值/列中最大值)*权重
    */
    // 店铺客户月均
    val storeCusMonthAvg: DataFrame = spark.table("ec_dwd.transaction")
      .groupBy("store_id", "customer_id", "tran_year", "tran_month")
      .agg(
        sum("price").as("sum_amount"),
        count("transaction_id").as("sum_count")
      )
      .groupBy("store_id", "customer_id")
      .agg(
        avg("sum_amount").cast(DecimalType(10, 2)).as("month_avg_amount"),
        avg("sum_count").cast(DecimalType(10, 2)).as("month_avg_count")
      )
    val tranTmp: DataFrame = spark.table("ec_dwd.transaction")
      .withColumn("max_date", max($"tran_dt").over(Window.orderBy($"tran_dt".desc)))
      .cache()
    // 近三个月
    val recent3Month: DataFrame = tranTmp
      .where(datediff($"max_date", $"tran_dt") <= 90) // 近三个月
      .groupBy("store_id", "customer_id")
      .agg(
        sum("price").cast(DecimalType(10, 2)).as("sum_amount_recent_3_month"),
        avg("transaction_id").cast(DecimalType(10, 2)).as("sum_count_recent_3_month")
      )
    // 近三周
    val recent3Week: DataFrame = tranTmp
      .where(datediff($"max_date", $"tran_dt") <= 21) // 近三个月
      .groupBy("store_id", "customer_id")
      .agg(
        sum("price").cast(DecimalType(10, 2)).as("sum_amount_recent_3_week"),
        avg("transaction_id").cast(DecimalType(10, 2)).as("sum_count_recent_3_week")
      )
    val storeCusTmp: DataFrame = spark.table("ec_dwd.transaction")
      .groupBy("store_id", "customer_id")
      .agg(
        sum("price").as("sum_amount"), //花钱额度
        count("transaction_id").as("sum_count") //购买次数
      )
      .join(storeCusMonthAvg, Seq("store_id", "customer_id"))
      .join(recent3Month, Seq("store_id", "customer_id"))
      .join(recent3Week, Seq("store_id", "customer_id"))
      .cache()
    // 综合指标【1以内小数】
    // (每个值/该列最大值)*权重【权重自己定】
    val storeCusMax: DataFrame = storeCusTmp
      .agg(
        // 先求出六个指标中的最大值
        max("sum_amount").as("max_sum_amount"),
        max("sum_count").as("max_sum_count"),
        max("month_avg_amount").as("max_month_avg_amount"),
        max("month_avg_count").as("max_month_avg_count"),
        max("sum_amount_recent_3_month").as("max_sum_amount_recent_3_month"),
        max("sum_count_recent_3_month").as("max_sum_count_recent_3_month"),
        max("sum_amount_recent_3_week").as("max_sum_amount_recent_3_week"),
        max("sum_count_recent_3_week").as("max_sum_count_recent_3_week"),
      )
    // 客户忠诚指数
    storeCusTmp
      .crossJoin(storeCusMax)
      .select($"store_id", $"customer_id",
        (
          ($"sum_amount" / $"max_sum_amount") * 0.07 +
            ($"sum_count" / $"max_sum_count") * 0.07 +
            ($"month_avg_amount" / $"max_month_avg_amount") * 0.08 +
            ($"month_avg_count" / $"max_month_avg_count") * 0.08 +
            ($"sum_amount_recent_3_month" / $"max_sum_amount_recent_3_month") * 0.15 +
            ($"sum_count_recent_3_month" / $"max_sum_count_recent_3_month") * 0.15 +
            ($"sum_amount_recent_3_week" / $"max_sum_amount_recent_3_week") * 0.2 +
            ($"sum_count_recent_3_week" / $"max_sum_count_recent_3_week") * 0.2
          ).cast(DecimalType(5, 4)).as("faith_factor")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.store_faith_factor")

    /**
     * 评分表 review
     */
    // 评价(覆盖率)
    val review: DataFrame = spark.table("ec_dwd.review").cache()
    spark.table("ec_dwd.transaction")
      .join(review, Seq("transaction_id"), "left")
      .agg(
        (count("review_score") / count("transaction_id"))
          .as("review_coverage")
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.review_coverage")

    // 评分(分布)
    spark.table("ec_dwd.review")
      .groupBy("review_score")
      .agg(count("*").as("review_distribution")) // 不同评分的总人数
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.review_distribution")

    // 评价(客户好评店铺分布)
    // 客户好评店铺分布：客户给出的最高评分是否在同一个店铺【总体】
    val cusStoreReview: DataFrame = spark.table("ec_dwd.review")
      .where($"review_score" >= 4) // 好评
      .as("R")
      .join(spark.table("ec_dwd.transaction").as("T"), "transaction_id")
      .groupBy("T.customer_id", "R.store_id")
      .agg(count("transaction_id").as("cus_store_good_cnt"))
      .cache()
    val cusReview: DataFrame = cusStoreReview
      .groupBy("customer_id")
      .agg(sum("cus_store_good_cnt").as("cus_good_cnt"))
      .where($"cus_good_cnt" > 3)
    cusStoreReview
      .join(cusReview, "customer_id")
      .select(
        $"customer_id", $"store_id",
        ($"cus_store_good_cnt" / $"cus_good_cnt").cast(DecimalType(3, 2)).as("cus_store_good_rate") // 好评覆盖率
      )
      .where($"cus_store_good_rate" >= 0.67)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.cus_good_review_distribution_of_store")

    // 评价(店铺的评分分布)
    val frmScores: DataFrame = spark
      .createDataFrame(Seq(Review(1), Review(2), Review(3), Review(4), Review(5)))
      .select($"review_score".cast(DecimalType(3, 2))) // 所有关于评分数据
    val frmStoreScores: DataFrame = spark.table("ec_dwd.review")
      .groupBy("store_id", "review_score")
      .agg(count("*").as("review_score_cnt")) // 店铺评分数量
    spark.table("ec_dwd.store")
      .crossJoin(frmScores)
      .as("L")
      .join(frmStoreScores.as("R"), Seq("store_id", "review_score"), "left")
      .withColumn(
        "review_score_cnt",
        when($"review_score_cnt".isNull, 0).otherwise($"review_score_cnt")
      )
      .select($"store_name", $"L.review_score", $"review_score_cnt")
      .orderBy("store_name", "L.review_score")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc") // 列式存储
      .saveAsTable("ec_dws.review_distribution_of_store")

    spark.stop()
    logger.info("EC DWS FINISHED")
  }
}
