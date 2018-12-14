package cn.tarena.dao;

import cn.tarena.pojo.Tongji;
import cn.tarena.pojo.Tongji2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlDao {

    public static void saveToMysql(Tongji t) throws Exception {
        //--1.注册mysql驱动
        //--2.获取数据库的链接对象
        //--3.执行插入
        //--4.提交事务

        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://192.168.242.101:3306/weblog",
                "root", "root");
        PreparedStatement ps = conn.prepareStatement(
                "insert into tongji values(?,?,?,?,?,?)");
        ps.setDate(1, t.getTime());
        ps.setInt(2, t.getPv());
        ps.setInt(3, t.getUv());
        ps.setInt(4, t.getVv());
        ps.setInt(5, t.getNewIp());
        ps.setInt(6, t.getNewCust());

        ps.executeUpdate();
        conn.close();


    }

    public static void tickToMysql(Tongji2 t) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://192.168.242.101:3306/weblog",
                "root", "root");

        PreparedStatement ps = conn.prepareStatement(
                "insert into tongji2 values(?,?,?,?)");

        ps.setDate(1, t.getTime());
        ps.setDouble(2, t.getBr());
        ps.setDouble(3, t.getAvgtime());
        ps.setDouble(4, t.getAvgdeep());

        ps.executeUpdate();
        conn.close();

    }

}
