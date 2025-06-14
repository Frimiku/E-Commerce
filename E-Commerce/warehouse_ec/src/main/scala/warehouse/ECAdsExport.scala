package warehouse

import core.{MysqlConfigFactory, SparkFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ECAdsExport {
  val logger = Logger.getLogger(ECAdsExport.getClass)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkFactory()
      .build()
      .baseConfig("ec_ads_report")
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

    import core.MysqlConfigFactory.Getter
    val dbName: String = "ec_ads_report" // 库名
    // mysql配置
    val conf: Getter = MysqlConfigFactory()
      .build()
      .setDriver("com.mysql.cj.jdbc.Driver")
      .setUrl(s"jdbc:mysql://single:3306/$dbName?createDatabaseIfNotExist=true&useUnicode=true&charSet=utf8") // 库名+建库
      .setUser("root_user") // 远程用户
      .setPassword("password") // 远程密码
      .finish()

    // 将ADS中库导入mysql中，同命库同名表
    spark.sql(s"show tables from $dbName")
      .collect() // 转变：DataFrame => Array[Row]，脱离sparkSession环境，变为一个普通数组
      .foreach(row => {
        val tableName: String = row.getString(1) // 表名
        spark.table(s"$dbName.$tableName") // 获取hbase中所有表（循环）
          .repartition(1)
          // 将表写入mysql中
          .write
          .jdbc(conf.getUrl, tableName, conf.getConf)
      })

    spark.stop()
    logger.info("EC ADS EXPORT TO MYSQL FINISHED")
  }
}
