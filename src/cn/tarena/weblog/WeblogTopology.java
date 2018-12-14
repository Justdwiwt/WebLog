package cn.tarena.weblog;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

public class WeblogTopology {

    public static void main(String[] args) throws Exception {
        //--创建Storm的环境参数对象
        Config conf = new Config();

        //--创建zk连接地址对象。可以不用全写，底层可以通过一台服务器找到整个的zk服务列表
        BrokerHosts hosts = new ZkHosts("hadoop01:2181,hadoop02:2181,hadoop03:2181");
        //--1参:zk连接地址  2参:消费的主题
        //--3参和4参:为了满足ack确认机制，会把相关的信息存储到指定的zk路径下
        //--下面表示：信息存储在zk的/weblog/info 路径下
        SpoutConfig sc = new SpoutConfig(hosts, "weblog", "/weblog", "info");
        //--指定Storm消费的数据类型为普通的文本类型
        sc.scheme = new SchemeAsMultiScheme(new StringScheme());

        //--获取Kafka的数据源，通过此数据源从Kafka消费数据
        KafkaSpout spout = new KafkaSpout(sc);
        ClearBolt clearBolt = new ClearBolt();
        PvBolt pvBolt = new PvBolt();
        UvBolt uvBolt = new UvBolt();
        VvBolt vvBolt = new VvBolt();
        HBaseBolt hbaseBolt = new HBaseBolt();

        PrintBolt printBolt = new PrintBolt();

        TopologyBuilder builder = new TopologyBuilder();
        //--绑定数据源
        builder.setSpout("kafkaSpout", spout);
        builder.setBolt("clearBolt", clearBolt, 2).setNumTasks(4).shuffleGrouping("kafkaSpout");
        builder.setBolt("pvBolt", pvBolt).globalGrouping("clearBolt");
        builder.setBolt("uvBolt", uvBolt).globalGrouping("pvBolt");
        builder.setBolt("vvBolt", vvBolt).globalGrouping("uvBolt");

        builder.setBolt("hbaseBolt", hbaseBolt).globalGrouping("vvBolt");
        builder.setBolt("printBolt", printBolt).globalGrouping("vvBolt");

        //--创建拓扑对象
        StormTopology topology = builder.createTopology();

        //--创建本地集群对象，用于测试
        //LocalCluster cluster=new LocalCluster();

        //--创建集群模式
        StormSubmitter cluster = new StormSubmitter();
        //noinspection AccessStaticViaInstance
        cluster.submitTopology("weblogTopolgoy01", conf, topology);
    }
}
