package warehouse

import core.MysqlConfigFactory.Getter
import core.{MysqlConfigFactory, SparkFactory}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.json_tuple
import org.apache.spark.sql.{SaveMode, SparkSession}

object ECOds {
  val logger = Logger.getLogger(ECOds.getClass)
  case class Line(json:String)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkFactory()
      .build()
      .baseConfig("ec_ods")
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

    val mysql: Getter = MysqlConfigFactory()
      .build()
      .setDriver("com.mysql.cj.jdbc.Driver")
      .setUrl("jdbc:mysql://single:3306/ebs?useUnicode=true&charSet=utf8") // 库名
      .setUser("root_user") // 远程用户
      .setPassword("password") // 远程密码
      .finish()

    /**
     * 创建近源层仓库(hive)：ec_ods
     */
    spark.sql("create database if not exists ec_ods")

    /**
     * 维度数据入库：
     * 分别从关系型数据库 mysql【single:3306】的ebs库中读取【维度表】 => [mysql数据导入hive中]
     * 1、用户信息表 customer 写入 ec_ods.customer
     * 2、店铺信息表 store 写入 ec_ods.store
     * */
    // 1、customer表 和 store表
    val tables = Array("customer", "store")
    tables.foreach(table => {
      // mysql中批量读取数据【数据全量读取】=> 有表头存在
      spark
        .read
        .jdbc(mysql.getUrl, table, mysql.getConf)
        .write
        .mode(SaveMode.Overwrite) // Append是增加，Overwrite是覆盖
        .format("orc") // 存储数据格式
        .saveAsTable(s"ec_ods.$table") // 将mysql中表存储至hive中
    })

    /**
     * 行为日志数据入库：
     * 分别 Flume 采集到 hdfs 的目录中external_commerce目录下的trasaction和review中读取数据，
     * 因为数据格式为 json 格式字符串，为了方便进一步清洗和扩展，直接将字段提取
     * 1、目录 transaction 中的数据写入数仓 ebs.transaction
     * 2、目录 review 中的数据写入数仓 ebs.review
     */
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    // 2、transaction表
    val rddLines1: RDD[Line] = sc
      .textFile("hdfs://single:9000/external_commerce/transaction", 4)
      .mapPartitions(_.map(line => Line(line)))
    spark
      .createDataFrame(rddLines1)
      // json解析
      .select(
        json_tuple($"json", "transaction_id", "customer_id", "store_id", "price", "product", "date", "time")
          .as(Seq("transaction_id", "customer_id", "store_id", "price", "product", "date", "time"))
      )
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ec_ods.transaction")

    // 3、review表
    val rddLines2: RDD[Line] = sc
      .textFile("hdfs://single:9000/external_commerce/review", 4)
      .mapPartitions(_.map(line => Line(line)))
    spark
      .createDataFrame(rddLines2)
      // json解析
      .select(
        json_tuple($"json", "transaction_id", "store_id", "review_score")
          .as(Seq("transaction_id", "store_id", "review_score"))
      )
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ec_ods.review")

    spark.stop()
    logger.info("EC ODS FINISHED")
  }
}
