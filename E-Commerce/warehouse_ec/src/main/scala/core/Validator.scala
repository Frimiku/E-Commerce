package core

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import scala.collection.mutable.ArrayBuffer

object Validator {

  /**
   * 数据校验【安全性判断】
   * @param title 校验主题[配置项名称]
   * @param value 待校验值
   * @param regex 若待校验值为字符串，且有特定的规则，提供正则表达式进一步验证格式
   */
  def check(title:String,value:Any,regex:String=null)={
    // 空指针判断
    if (null==value){
      throw new RuntimeException(s"value for $title null pointer exception")
    }
    // value为字符串时，判断是否为空
    if (value.isInstanceOf[String]){
      if (value.toString.isEmpty){
        throw new RuntimeException(s"value for $title empty string exception")
      }
      // 正则匹配与否判断
      if (null != regex && !value.toString.matches(regex)){
        throw new RuntimeException(s"value for $title not match regex $regex exception")
      }
    }
  }

  /**
   * 日期维度数据生成：从开始时间到结束时间的期间时间生成
   */
  case class DimDate(tran_year:Int,tran_quarter: Int,tran_month:Int,tran_month_week:Int,tran_day:Int,tran_range:Int)
  // 参数：开始日期，结束时间
  def dimDate(dateBegin:String,dateEnd:String):Seq[DimDate] = {
    var begin = LocalDateTime.parse(s"${dateBegin}T00:00:00")
    val end = LocalDateTime.parse(s"${dateEnd}T00:00:00")
    val buffer = ArrayBuffer[DimDate]()
    // isBefore => 小于
    while(begin.isBefore(end)){
      val year: Int = begin.getYear
      val month: Int = begin.getMonthValue
      val quarter: Int = (month - 1) / 3 + 1
      val monthWeek: Int = begin.get(ChronoField.ALIGNED_WEEK_OF_MONTH)
      val day: Int = begin.getDayOfMonth
      val range: Int = begin.getHour/3+1
      buffer.append(DimDate(year,quarter,month,monthWeek,day,range))
      begin = begin.plusHours(3)
    }
    buffer
  }

  /**
   * UDF: 修补日期，转为标准格式
   */
  def dateFormat(datetime:String) :String= {
    val arr: Array[String] = datetime.split(" ")
    //年月日
    val ymd: String = arr(0)
      .split("-")
      .map(_.toInt)
      .map(d => s"${if (d < 10) "0" else ""}$d")
      .mkString("-")

    // 时分秒
    var bool = false
    val hms: String = arr(1)
      .split(":")
      .map(_.toInt)
      .zipWithIndex
      .map(d =>
        if (d._2 == 0 && d._1 >= 24) {
          bool = true
          d._1 - 24
        }
        else
          d._1
      )
      .map(d => s"${if (d < 10) "0" else ""}$d")
      .mkString(":")
    // 拼接为一个标准日期
    val date: String = Array(ymd, hms).mkString("T")

    LocalDateTime
      .parse(date)
      .plusDays(if(bool) 1 else 0)
      .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  }

}
