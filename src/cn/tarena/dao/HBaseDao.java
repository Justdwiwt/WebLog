package cn.tarena.dao;

import cn.tarena.pojo.FluxInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HBaseDao {

    public static void saveToHBase(FluxInfo f) throws Exception {
        //--获取HBase的环境参数对象
        Configuration conf = HBaseConfiguration.create();
        //--设置zk集群的地址，可以不用全写
        conf.set("hbase.zookeeper.quorum",
                "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        HTable table = new HTable(conf, "weblog");

        //--本业务中，行键:sstime_uvid_ssid_100以内的随机数
        //--设计行键时遵循的规则：1.有意义 2.散列原则，避免数据倾斜 3.不宜过长
        String rowKey = f.getSstime() + "_" + f.getUvid() + "_" + f.getSsid() + "_" + (int) (Math.random() * 100);
        Put put = new Put(rowKey.getBytes());

        put.add("cf1".getBytes(), "url".getBytes(), f.getUrl().getBytes());
        put.add("cf1".getBytes(), "urlname".getBytes(), f.getUrlname().getBytes());
        put.add("cf1".getBytes(), "uvid".getBytes(), f.getUvid().getBytes());
        put.add("cf1".getBytes(), "ssid".getBytes(), f.getSsid().getBytes());
        put.add("cf1".getBytes(), "sscount".getBytes(), f.getSscount().getBytes());
        put.add("cf1".getBytes(), "sstime".getBytes(), f.getSstime().getBytes());
        put.add("cf1".getBytes(), "cip".getBytes(), f.getCip().getBytes());

        //--执行插入
        table.put(put);
    }

    @SuppressWarnings("Duplicates")
    public static List<FluxInfo> queryByRange(String startTime, String endtime, String regex) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum",
                "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        HTable table = new HTable(conf, "weblog");
        Scan scan = new Scan();
        scan.setStartRow(startTime.getBytes());
        scan.setStopRow(endtime.getBytes());

        //--获取正则匹配的行键过滤器
        Filter filter = new RowFilter(CompareOp.EQUAL,
                new RegexStringComparator(regex));
        //--设置过滤器生效
        scan.setFilter(filter);
        //--得到查询的结果集
        ResultScanner rs = table.getScanner(scan);
        //--获取行的迭代器，每行数据封装到Result对象里
        Iterator<Result> it = rs.iterator();

        //--用于封装行数据的结果集
        List<FluxInfo> results = new ArrayList<>();
        while (it.hasNext()) {
            //--获取一行对象
            Result row = it.next();
            String url = new String(row.getValue("cf1".getBytes(), "url".getBytes()));
            String urlname = new String(row.getValue("cf1".getBytes(), "urlname".getBytes()));
            String uvid = new String(row.getValue("cf1".getBytes(), "uvid".getBytes()));
            String ssid = new String(row.getValue("cf1".getBytes(), "ssid".getBytes()));
            String sscount = new String(row.getValue("cf1".getBytes(), "sscount".getBytes()));
            String sstime = new String(row.getValue("cf1".getBytes(), "sstime".getBytes()));
            String cip = new String(row.getValue("cf1".getBytes(), "cip".getBytes()));
            FluxInfo f = new FluxInfo();
            f.setUrl(url);
            f.setUrlname(urlname);
            f.setUvid(uvid);
            f.setSsid(ssid);
            f.setSscount(sscount);
            f.setSstime(sstime);
            f.setCip(cip);

            results.add(f);
        }

        return results;
    }

    @SuppressWarnings("Duplicates")
    public static List<FluxInfo> queryByColumn(String familyName, String colName, String colValue) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum",
                "hadoop01:2181,hadoop02:2181,hadoop03:2181");
        HTable table = new HTable(conf, "weblog");
        Scan scan = new Scan();
        Filter filter = new SingleColumnValueFilter(familyName.getBytes(),
                colName.getBytes(),
                CompareOp.EQUAL, colValue.getBytes());
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);

        //--获取行的迭代器，每行数据封装到Result对象里
        Iterator<Result> it = rs.iterator();

        //--用于封装行数据的结果集
        List<FluxInfo> results = new ArrayList<>();
        while (it.hasNext()) {
            //--获取一行对象
            Result row = it.next();
            String url = new String(row.getValue("cf1".getBytes(), "url".getBytes()));
            String urlname = new String(row.getValue("cf1".getBytes(), "urlname".getBytes()));
            String uvid = new String(row.getValue("cf1".getBytes(), "uvid".getBytes()));
            String ssid = new String(row.getValue("cf1".getBytes(), "ssid".getBytes()));
            String sscount = new String(row.getValue("cf1".getBytes(), "sscount".getBytes()));
            String sstime = new String(row.getValue("cf1".getBytes(), "sstime".getBytes()));
            String cip = new String(row.getValue("cf1".getBytes(), "cip".getBytes()));
            FluxInfo f = new FluxInfo();
            f.setUrl(url);
            f.setUrlname(urlname);
            f.setUvid(uvid);
            f.setSsid(ssid);
            f.setSscount(sscount);
            f.setSstime(sstime);
            f.setCip(cip);

            results.add(f);
        }

        return results;

    }

}
