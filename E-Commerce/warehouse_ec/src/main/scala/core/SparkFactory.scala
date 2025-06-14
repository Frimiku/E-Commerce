package core

import core.SparkFactory.Builder
import core.Validator.check
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// 一般而言：将数据导入【入口】，将hive中数据导入进行操作[多进一出] | 出口
// hive与spark连接[spark和hive启起来]
class SparkFactory {
  def build():Builder={
    new Builder {
      val conf:SparkConf = new SparkConf()

      /**
       * 单项配置
       * @param item 配置项名称
       * @param value 配置项值
       * @param regexValue 配置项值正则规则
       */
      private def set(item:String,value:String,regexValue:String=null)={
        check("name_of_config_item",item,"^spark\\..*")
        check(item,value,regexValue)
        conf.set(item,value)
      }

      // 基本配置
      private def setBaseAppName(appName:String)={
        set("spark.app.name",appName,"^\\w+$")
      }
      private def setBaseMaster(master:String)={
        set("spark.master",master,"local(\\[(\\*|[1-9][0-9]*)])?|spark://([a-z]\\w+|\\d{1,3}(\\.\\d{1,3}){3}):\\d{4,5}|yarn")
      }
      private def setBaseDeployMode(deployMode:String) ={
        set("spark.submit.deployMode",deployMode,"client|cluster")
      }
      private def setBaseEventLogEnabled(eventLogEnabled:Boolean) ={
        set("spark.eventLog.enabled",s"$eventLogEnabled")
      }
      def baseConfig(appName:String,master:String="local[*]",deployMode:String="client",eventLogEnabled:Boolean=false):Builder={
        setBaseAppName(appName)
        setBaseMaster(master)
        /*
         *  在集群中（standalone|yarn）某一台机器上发布驱动端：
         *  client：本地模式
         *  cluster：远程模式
         */
        setBaseDeployMode(deployMode)
        /*
         * 构建 Web UI
         */
        setBaseEventLogEnabled(eventLogEnabled)
        this
      }

      // 驱动端优化
      private def setDriverMemory(memoryGB:Int)={
        set("spark.driver.memory",s"${memoryGB}g","[1-9]\\d*g")
      }
      private def setDriverCoreNum(coreNum:Int)={
        set("spark.driver.cores",s"$coreNum","[1-9]\\d*")
      }
      private def setDriverMaxResultGB(maxRstGB:Int)={
        set("spark.driver.maxResultSize",s"${maxRstGB}g","[1-9]\\d*g")
      }
      private def setDriverHost(driverHost:String)={
        set("spark.driver.host",driverHost,"localhost|[a-z]\\w+")
      }
      def optimizeDriver(memoryGB:Int=2,coreNum:Int=1,maxRstGB:Int=1,driverHost:String="localhost"):Builder={
        setDriverMemory(memoryGB)
        setDriverCoreNum(coreNum)
        // 每一个spark行动算子触发的所有分区序列化结果大小上线
        setDriverMaxResultGB(maxRstGB)
        // standalone 模式需要配置 driverHost，便于 executor 与 master 通信
        if (conf.get("spark.master").startsWith("spark://")){
          setDriverHost(driverHost)
        }
        this
      }

      // 执行端优化
      private def setExecutorMemory(memoryGB:Int)={
        set("spark.executor.memory",s"${memoryGB}g","[1-9]\\d*g")
      }
      private def setExecutorCoreNum(coreNum:Int)={
        set("spark.executor.cores",s"$coreNum","[1-9]\\d*")
      }
      def optimizeExecutor(memoryGB:Int=1,coreNum:Int=1):Builder={
        setExecutorMemory(memoryGB)
        /*
         * yarn模式下只能 1 个核
         * 其他模式下，所有可用的核
         */
        if (conf.get("spark.master").startsWith("spark://")){
          setExecutorCoreNum(coreNum)
        }
        this
      }

      // 限制优化
      private def setLimitMaxCores(maxCores:Int)={
        set("spark.cores.max",s"$maxCores","[1-9]\\d*")
      }
      private def setLimitMaxTaskFailure(maxTaskFailure:Int)={
        set("spark.task.maxFailures",s"$maxTaskFailure","[1-9]\\d*")
      }
      private def setLimitMaxLocalWaitS(maxLocalWaitS:Int)={
        set("spark.locality.wait",s"${maxLocalWaitS}s","[1-9]\\d*s")
      }
      def optimizeLimit(maxCores:Int=4,maxTaskFailure:Int=3,maxLocalWaitS:Int=3):Builder={
        if (conf.get("spark.master").startsWith("spark://")){
          setLimitMaxCores(maxCores)
        }
        /**
         * 单个任务允许失败的最大次数，超出会杀死本次 job ，重试，最大重试次数为：此值-1
         */
        setLimitMaxTaskFailure(maxTaskFailure)
        /**
         * 数据本地化读取加载的最大等待时间
         * 大任务：建议适当增加此值
         */
        setLimitMaxLocalWaitS(maxLocalWaitS)
        this
      }

      // 序列化优化
      def optimizeSerializer(serde:String="org.apache.spark.serializer.JavaSerializer", clas:Array[Class[_]]=null):Builder={

        /**
         * 设置将需要通过网络发送或快速缓存的对象序列化的工具类
         * 默认为 Java 序列化：org.apache.spark.serializer.JavaSerializer
         * 为了提速，推荐设置为：org.apache.spark.serializer.KryoSerializer
         * 若采用 KryoSerializer 序列化方式，需要将所有自定义的实体类(样例类)注册到配置中心
         */
        set("spark.serializer",serde,"([a-z]+\\.)+[A-Z]\\w*")
        if (serde.equals("org.apache.spark.serializer.KryoSerializer")){
          conf.registerKryoClasses(clas)
        }
        this
      }

      // 网络相关优化
      private def setNetTimeout(netTimeoutS:Int)={
        set("spark.network.timeout",s"${netTimeoutS}s","[1-9]\\d*s")
      }
      private def setNetSchedulerMode(schedulerMode:String) ={
        set("spark.scheduler.mode",schedulerMode,"FIFO|FAIR")
      }
      def optimizeNetAbout(netTimeoutS:Int=120,schedulerMode:String="FIFO"):Builder={
        /**
         * 所有和网络交互相关的超时阈值
         */
        setNetTimeout(netTimeoutS)
        /**
         * 多人工作模式下：建议设置为 FAIR
         */
        setNetSchedulerMode(schedulerMode)
        this
      }

      // 动态分配优化
      private def setDynamicEnabled(dynamicEnabled:Boolean) ={
        set("spark.dynamicAllocation.enabled",s"$dynamicEnabled")
      }
      private def setDynamicInitialExecutors(initialExecutors:Int)={
        set("spark.dynamicAllocation.initialExecutors",s"$initialExecutors","[1-9]\\d*")
      }
      private def setDynamicMaxExecutors(maxExecutors:Int)={
        set("spark.dynamicAllocation.maxExecutors",s"$maxExecutors","[1-9]\\d*")
      }
      private def setDynamicMinExecutors(minExecutors:Int)={
        set("spark.dynamicAllocation.minExecutors",s"$minExecutors","[0-9]\\d*")
      }
      def optimizeDynamicAllocation(dynamicEnabled:Boolean=false,initialExecutors:Int=3,minExecutors:Int=0,maxExecutors:Int=6):Builder={
        // 只有 dynamicEnabled为true才可继续进行
        /**
         * 根据应用的工作需求，动态分配 Executors 资源
         */
        if (dynamicEnabled){
          setDynamicEnabled(true)
          setDynamicInitialExecutors(initialExecutors)
          setDynamicMinExecutors(minExecutors)
          setDynamicMaxExecutors(maxExecutors)
        }
        this
      }

      // shuffle优化
      private def setShuffleParallelism(parallelism:Int)={
        set("spark.default.parallelism",s"$parallelism","[1-9]\\d*")
      }
      private def setShuffleCompressEnabled(shuffleCompressEnabled:Boolean)={
        set("spark.shuffle.compress",s"$shuffleCompressEnabled")
      }
      private def setShuffleMaxSizePerReducer(maxSizeMB:Int)={
        set("spark.reducer.maxSizeInFlight",s"${maxSizeMB}m","[1-9]\\d*m")
      }
      private def setShuffleServiceEnabled(shuffleServiceEnabled:Boolean)={
        set("spark.shuffle.service.enabled",s"$shuffleServiceEnabled")
      }
      def optimizeShuffle(parallelism:Int=3,shuffleCompressEnabled:Boolean=false,maxSizeMB:Int=128,shuffleServiceEnabled:Boolean=true):Builder={
        /**
         * 如果用户没有指定分区数（numPar|Partitioner），则采用该值作为默认的分区数
         */
        setShuffleParallelism(parallelism)
        /**
         * Shuffle 过程中 Map 端输出数据是否压缩，建议生成过程中，数据规模较大时启动
         */
        setShuffleCompressEnabled(shuffleServiceEnabled)
        /**
         * 设置Reducer端缓冲区大小,生成环境中，服务器内存较大时，可以适当增大【本地计算时需关闭，为false】
         */
        setShuffleMaxSizePerReducer(maxSizeMB)
        /**
         * 开启一个独立外部服务，专门存储 Executor 产生的数据
         */
        setShuffleServiceEnabled(shuffleServiceEnabled)
        this
      }

      // 推测执行
      private def setSpeculationEnabled(speculationEnabled:Boolean)={
        set("spark.speculation",s"$speculationEnabled")
      }
      private def setSpeculationIntervalS(intervalS:Int)={
        set("spark.speculation.interval",s"${intervalS}s","[1-9]\\d*s")
      }
      private def setSpeculationQuantile(quantile:Float)={
        set("spark.speculation.quantile",s"${quantile}","0?\\.\\d+")
      }
      def optimizeSpeculation(speculationEnabled:Boolean=false,intervalS:Int=5,quantile:Float=0.75f):Builder={
        if (speculationEnabled){
          /**
           * 是否开启推测执行服务,将各阶段中(Stage)中执行慢的任务(Task)重启
           */
          setSpeculationEnabled(true)
          /**
           * 推测频次 intervalS(秒/次)
           */
          setSpeculationIntervalS(intervalS)
          /**
           * 开启推测执行前，任务的完成比列
           */
          setSpeculationQuantile(quantile)
        }
        this
      }

      // 运行时优化
      // 数据倾斜优化
      private def setAdaptiveEnabled(adaptiveEnabled:Boolean)={
        set("spark.sql.adaptive.enabled",s"$adaptiveEnabled")
      }
      // 设置自动广播大小
      private def setAutoBroadCastThreshold(thresholdMB:Int)={
        set("spark.sql.autoBroadcastJoinThreshold",s"${thresholdMB}MB","[1-9]\\d*MB")
      }
      // 根据消耗情况来判断是否进行优化（默认关闭） => 一般打开【了解】
      private def setCBOEnabled(enabled:Boolean)={
        set("spark.sql.cbo.enabled",s"$enabled")
      }
      def optimizeRuntime(adaptiveEnabled:Boolean=false, threshold:Int=10, cboEnabled:Boolean)={
        /**
         * spark.sql.adaptive.skewJoin.enabled 默认为 true
         * 但只有当 spark.sql.adaptive.enabled(默认为 false) 设置为 true 时生效
         */
        setAdaptiveEnabled(adaptiveEnabled)
        setAutoBroadCastThreshold(threshold)
        setCBOEnabled(cboEnabled)
        this
      }

      // SparkStreaming背压机制
      // 是否开启背压机制(默认false)
      private def setBackpressureEnabled(backpressureEnabled:Boolean)={
        set("spark.streaming.backpressure.enabled",s"$backpressureEnabled")
      }
      // 全局设置，限制所有接收器的最大接受频率
      private def setBackpressureReceiverMaxRate(receiverMaxRate:Int)={
        set("spark.streaming.receiver.maxRate",s"$receiverMaxRate")
      }
      // 针对kafka特定设置，限制每个kafka分区的最大接受速率
      private def setBackpressureMaxRatePerPartition(maxRatePerPartition:Int)={
        set("spark.streaming.kafka.maxRatePerPartition",s"$maxRatePerPartition")
      }
      def backpressure(backpressureEnabled:Boolean=false, receiverMaxRate:Int=5000, maxRatePerPartition:Int=1500)={
        setBackpressureEnabled(backpressureEnabled)
        setBackpressureReceiverMaxRate(receiverMaxRate)
        setBackpressureMaxRatePerPartition(maxRatePerPartition)
        this
      }

      // 数仓路径(根目录在哪里)
      def warehouseDir(hdfs:String):Builder={
        set("spark.sql.warehouse.dir",hdfs,"hdfs://([a-z]\\w+|\\d{1,3}(\\.\\d{1,3}){3}):\\d{4,5}(/\\w+)+")
        this
      }

      // 生成SparkSession
      def end():SparkSession={
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport() // 用途：sparkSql关联hive【需要四个配置】
          .getOrCreate()
      }

    }
  }
}
object SparkFactory{
  def apply(): SparkFactory = new SparkFactory()

  trait Builder{
    // 基本配置
    /**
     * 基本配置
     * @param appName 名称
     * @param master
     * @param deployMode  发布者模式
     * @param eventLogEnabled 事件日志模式【发布时打开,测试时无需打开】
     * @return
     */
    def baseConfig(appName:String,master:String="local[*]",deployMode:String="client",eventLogEnabled:Boolean=false):Builder
    // 以下为优化配置
    /**
     * 驱动端优化
     * @param memoryGB 内存gb
     * @param coreNum 核数
     * @param maxRstGB 最大结果大小
     * @param driverHost diver端配置
     * @return
     */
    def optimizeDriver(memoryGB:Int=2,coreNum:Int=1,maxRstGB:Int=1,driverHost:String="localhost"):Builder
    /**
     * 执行端优化
     * @param memoryGB 内存gb
     * @param coreNum 核数
     * @return
     */
    def optimizeExecutor(memoryGB:Int=1,coreNum:Int=1):Builder
    /**
     * 限制优化
     * @param maxCores 最大核数
     * @param maxTaskFailure 单个task任务失败的最大次数(容错)。超出会杀死本次 job ，重试，最大重试次数为：此值-1
     * @param maxLocalWaitS 最多等待时间（秒）
     * @return
     */
    def optimizeLimit(maxCores:Int=4,maxTaskFailure:Int=3,maxLocalWaitS:Int=3):Builder
    /**
     * 序列化优化
     * @param serde 全包路径（class文件的全包路径）
     * @param clas 多个class
     * @return
     */
    def optimizeSerializer(serde:String="org.apache.spark.serializer.JavaSerializer", clas:Array[Class[_]]=null):Builder
    /**
     * 网络相关优化
     * @param netTimeoutS 网络超时(秒)
     * @param schedulerMode 消息队列[如何分配给任务资源],此处是平等分配资源
     * @return
     */
    def optimizeNetAbout(netTimeoutS:Int=120,schedulerMode:String="FIFO"):Builder
    /**
     * 资源动态分配优化[按需(要)分配]
     * @param dynamicEnabled 是否开启动态分配
     * @param initialExecutors 初始化容器数
     * @param minExecutors 最小保留容器数
     * @param maxExecutors 最多容器数
     * @return
     */
    def optimizeDynamicAllocation(dynamicEnabled:Boolean=false,initialExecutors:Int=3,minExecutors:Int=0,maxExecutors:Int=6):Builder
    /**
     * shuffle优化
     * @param parallelism 并行度【分区数】
     * @param shuffleCompressEnabled 资源是否需要压缩
     * @param maxSizeMB 单词传输的字节上限[一个block大小]
     * @param shuffleServiceEnabled 是否开启shuffle，yarn跑的时候需要开启true【本地计算时需关闭，为false】
     * @return
     */
    def optimizeShuffle(parallelism:Int=3,shuffleCompressEnabled:Boolean=false,maxSizeMB:Int=128,shuffleServiceEnabled:Boolean=true):Builder
    /**
     * 推测执行（若检查其性能不行，干掉重开）
     * @param speculationEnabled 是否开启测试推测模式
     * @param intervalS 多长时间检查一次（秒），即：推测频次【定期得出一个均值】
     * @param quantile 开启推测执行前，任务的完成比列
     * @return
     */
    def optimizeSpeculation(speculationEnabled:Boolean=false,intervalS:Int=5,quantile:Float=0.75f):Builder
    /**
     * 运行时优化
     * @param adaptiveEnabled 数据倾斜优化
     * @param threshold 设置自动广播大小
     * @param cboEnabled 根据消耗情况来判断是否进行优化（默认关闭）=> 一般打开
     * @return
     */
    def optimizeRuntime(adaptiveEnabled:Boolean=false, threshold:Int=10, cboEnabled:Boolean):Builder
    /**
     * SparkStreaming背压机制
     * @param backpressureEnabled 是否开启背压机制(默认false)
     * @param receiverMaxRate 全局设置，限制所有接收器的最大接受频率
     * @param maxRatePerPartition 针对kafka特定设置，限制每个kafka分区的最大接受速率
     * @return
     */
    def backpressure(backpressureEnabled:Boolean=false, receiverMaxRate:Int=5000, maxRatePerPartition:Int=1500):Builder
    // 数仓路径
    def warehouseDir(hdfs:String):Builder
    // 生成SparkSession
    def end():SparkSession
  }
}
