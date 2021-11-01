

import java.sql.{Connection, DriverManager}


object CreateTimeTable{
   def main(args: Array[String]): Unit = {
     // 访问本地MySQL服务器，通过3306端口访问mysql数据库
     val url = "jdbc:mysql://192.168.126.129:3306/saas?useUnicode=true&characterEncoding=utf-8&useSSL=false"
     //驱动名称
     val driver = "com.mysql.jdbc.Driver"

     //用户名
     val username = "root"
     //密码
     val password = "123456"
     //初始化数据连接
     var connection: Connection = null
     try {
       //注册Driver
       Class.forName(driver)
       //得到连接
       connection = DriverManager.getConnection(url, username, password)
       val statement = connection.createStatement

       //    执行插入操作
       val rs2 = statement.executeUpdate("INSERT INTO `test` (cdate) VALUES ('2021-10-08')")
       println("插入数据完成")

     } catch {
       case e: Exception => e.printStackTrace
     }
     //关闭连接，释放资源
     connection.close

   }
 }
