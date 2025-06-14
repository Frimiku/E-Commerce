package warehouse

import core.SparkFactory
import core.Validator.dateFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object ECDwd {
  val logger = Logger.getLogger(ECDwd.getClass)
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val spark: SparkSession = SparkFactory()
      .build()
      .baseConfig("ec_dwd")
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
    /**
     * 创建明细层仓库(hive)：ec_dwd
     */
    spark.sql("create database if not exists ec_dwd")

    /**
     * 维度表: 用户表加载 customer:
     * 1、需要根据【指标列】需求进行行列裁剪
     * 2、提取所需字段(指标列)：编号,信用卡，职位，性别，国家，邮箱域名
     */
    spark
      .table("ec_ods.customer")
      .select($"customer_id", $"credit_no", $"job",
        $"gender", $"country", $"credit_type", $"last_name".as("customer_name"),
        regexp_extract($"email", ".*@(.*)", 1).as("email")
      )
      //存盘至hive的dwd数据库中
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ec_dwd.customer")

    /**
     * 维度表: 商铺表加载 store
     * 1、字段全部保留
     * 2、提取所需字段(指标列)：商铺，商铺名称，员工数
     */
    spark
      .table("ec_ods.store")
      .select($"store_id", $"store_name", $"employee_number"
      )
      .where($"store_id".isNotNull)
      //存盘至hive的dwd数据库中
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ec_dwd.store")

    /**
     * 事实表：交易表加载 transaction
     * 1、清洗重复的交易编号
     * 2、将日期和时间合并成标准格式 yyyy-MM-dd HH:mm:ss
     * 3、日期维度细化：年，季，月，周，日，时段(8)
     * 4、按年季月分区
     */
    // 交易表transaction
    // UDF:注册
    spark.udf.register("my_date_format", (datetime: String) => dateFormat(datetime))
    val tran: DataFrame = spark
      .table("ec_ods.transaction")
      // 转换类型
      .select(
        $"transaction_id".cast(IntegerType),
        $"customer_id".cast(IntegerType),
        $"store_id".cast(IntegerType),
        $"price".cast(DecimalType(10, 2)),
        $"product",
        // 2、将日期和时间合并成标准格式 yyyy-MM-dd HH:mm:ss
        callUDF("my_date_format", concat_ws(" ", $"date", $"time"))
          .as("tran_dt")
      )
      .cache()

    // 所有的数据，开窗口：2为重复数据，1为不重复数据 => 窗口函数都是需要排序的order by
    val allTran: DataFrame = tran
      .withColumn("rnk", // 排名
        row_number()
          .over(Window.partitionBy($"transaction_id").orderBy($"tran_dt"))
      ).cache()

    // 有重复的数据
    val tranRepeat: DataFrame = tran
      .groupBy($"transaction_id")
      .count()
      .where($"count" > 1)

    // 不重复数据
    val tranUnrepeated: Dataset[Row] = allTran
      .where($"rnk" === 1)
      .select(
        $"transaction_id",
        $"customer_id",
        $"store_id",
        $"price",
        $"product",
        $"tran_dt"
      )
      .cache()

    // 在不重复数据中寻找最大的号
    val maxTranId: DataFrame = tranUnrepeated
      .agg(max($"transaction_id").as("max_tran_id"))

    // 1、清洗重复的交易编号
    val tranRepeated: DataFrame = allTran
      // 重复的数据
      .where($"rnk" > 1)
      .withColumn("rn", // 行号
        row_number().over(Window.orderBy($"transaction_id")))
      .as("A")
      .crossJoin(maxTranId.as("M")) // 笛卡尔积
      // 最大值+行号:withColumn中同名列,覆盖原表字段
      //.withColumn("transaction_id",$"M.max_tran_id" + $"A.rn")
      .select(
        ($"M.max_tran_id" + $"A.rn").as("transaction_id"),
        $"customer_id",
        $"store_id",
        $"price",
        $"product",
        $"tran_dt"
      )

    tranUnrepeated
      .unionAll(tranRepeated)
      // 3、日期维度细化：年，季，月，周(月周)，日(月日)，时段(8)
      .select(
        $"transaction_id",
        $"customer_id",
        $"store_id",
        $"price",
        $"product",
        $"tran_dt",
        year($"tran_dt").as("tran_year"),
        quarter($"tran_dt").as("tran_quarter"),
        month($"tran_dt").as("tran_month"),
        ceil(dayofmonth($"tran_dt") / 7).as("tran_month_week"), // 月周
        dayofmonth($"tran_dt").as("tran_day"),
        floor(hour($"tran_dt") / 3 + 1).as("tran_range") // 3小时一个时段
      )
      .repartition(1)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      // 4、按年季月分区
      .partitionBy("tran_year", "tran_quarter", "tran_month")
      .saveAsTable("ec_dwd.transaction")


    /**
     * 事实表：评价表加载 review
     * 1、同一个交易编号在交易表中的店铺号和评价表中的店铺号不一致，以交易表为准
     * 2、评价残缺
     */
    // 评价表review
    val tranDwd: DataFrame = spark.table("ec_dwd.transaction")
    val review: DataFrame = spark
      .table("ec_ods.review")
      // 转换类型
      .select(
        $"transaction_id".cast(IntegerType),
        $"store_id".cast(IntegerType),
        $"review_score".cast(DecimalType(3, 2))
      )
      .as("R")
      //1、同一个交易编号在交易表中的店铺号和评价表中的店铺号不一致，以交易表为准
      .join(tranDwd.as("T"), Seq("transaction_id"), "left")
      .withColumn("alia_store_id",
        when($"R.store_id".isNull, $"R.store_id").otherwise($"T.store_id")
      )
      .select($"transaction_id", $"alia_store_id".as("store_id"), $"review_score")
      .cache()
    // 平均评分
    val avgReviewScore: DataFrame = review
      .where($"review_score".isNotNull)
      .agg(avg($"review_score").cast(DecimalType(3, 2)).as("avg_score"))
    review
      .join(avgReviewScore)
      //2、评价残缺
      .withColumn("review_score",
        when($"review_score".isNull, $"avg_score")
          .otherwise($"review_score")
      )
      .select(
        $"transaction_id", $"store_id", $"review_score"
      )
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable("ec_dwd.review")

    spark.stop()
    logger.info("EC DWD FINISHED")
  }
}
