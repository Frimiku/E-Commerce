package core

import core.MysqlConfigFactory.{Getter, Setter}
import core.Validator.check

import java.util.Properties


// 一般而言：数据的出口【将结果数据导出至mysql中】[多进多出] | 出口
class MysqlConfigFactory{
  def build():Setter = {
    new Setter {

      val conf = new Properties()

      override def setDriver(driverCla: String): Setter = {
        check("name_of_mysql_driver_class",driverCla,"com\\.mysql(\\.cj)?\\.jdbc\\.Driver")
        conf.setProperty("driver",driverCla)
        this
      }

      override def setUrl(url: String): Setter = {
        check("url_to_connect_mysql",url,"jdbc:mysql://([a-z]\\w+|\\d{1,3}(\\.\\d{1,3}){3}):\\d{4,5}/[a-z]\\w+(\\?.+)?")
        conf.setProperty("url",url)
        this
      }

      override def setUser(user: String): Setter = {
        check("user_to_connect_mysql",user)
        conf.setProperty("user",user)
        this
      }

      override def setPassword(password: String): Setter = {
        check("password_to_connect_mysql",password)
        conf.setProperty("password",password)
        this
      }

      override def finish(): Getter = {
        new Getter {
          override def getUrl: String = conf.getProperty("url")

          override def getConf: Properties = conf
        }
      }
    }
  }
}
object MysqlConfigFactory{
  def apply(): MysqlConfigFactory = new MysqlConfigFactory()

  trait Getter{
    def getUrl:String
    def getConf:Properties
  }

  trait Setter{
    // 字节码文件全包路径
    def setDriver(driverCla:String):Setter
    def setUrl(url:String):Setter
    def setUser(user:String):Setter
    def setPassword(password:String):Setter
    def finish():Getter
  }
}
