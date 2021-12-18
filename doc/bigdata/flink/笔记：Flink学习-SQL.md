参考资料
[01_尚硅谷大数据技术之Flink.docx](https://www.yuque.com/attachments/yuque/0/2021/docx/1157748/1636292003823-5afacfa7-1b8a-4eb1-8bbc-ceeba64225db.docx?_lake_card=%7B%22src%22%3A%22https%3A%2F%2Fwww.yuque.com%2Fattachments%2Fyuque%2F0%2F2021%2Fdocx%2F1157748%2F1636292003823-5afacfa7-1b8a-4eb1-8bbc-ceeba64225db.docx%22%2C%22name%22%3A%2201_%E5%B0%9A%E7%A1%85%E8%B0%B7%E5%A4%A7%E6%95%B0%E6%8D%AE%E6%8A%80%E6%9C%AF%E4%B9%8BFlink.docx%22%2C%22size%22%3A9248911%2C%22type%22%3A%22application%2Fvnd.openxmlformats-officedocument.wordprocessingml.document%22%2C%22ext%22%3A%22docx%22%2C%22status%22%3A%22done%22%2C%22taskId%22%3A%22u05354010-49e6-42fb-8251-646a4d6e9e1%22%2C%22taskType%22%3A%22upload%22%2C%22id%22%3A%22uafab4c90%22%2C%22card%22%3A%22file%22%7D)
[02_尚硅谷大数据技术之FlinkSQL&TableAPI.docx](https://www.yuque.com/attachments/yuque/0/2021/docx/1157748/1636292003770-a5628d1e-0c10-409f-99a7-0c38b07c11c7.docx?_lake_card=%7B%22src%22%3A%22https%3A%2F%2Fwww.yuque.com%2Fattachments%2Fyuque%2F0%2F2021%2Fdocx%2F1157748%2F1636292003770-a5628d1e-0c10-409f-99a7-0c38b07c11c7.docx%22%2C%22name%22%3A%2202_%E5%B0%9A%E7%A1%85%E8%B0%B7%E5%A4%A7%E6%95%B0%E6%8D%AE%E6%8A%80%E6%9C%AF%E4%B9%8BFlinkSQL%26TableAPI.docx%22%2C%22size%22%3A1648757%2C%22type%22%3A%22application%2Fvnd.openxmlformats-officedocument.wordprocessingml.document%22%2C%22ext%22%3A%22docx%22%2C%22status%22%3A%22done%22%2C%22taskId%22%3A%22uf20089a6-e069-4b8c-9184-8cbfa284d4f%22%2C%22taskType%22%3A%22upload%22%2C%22id%22%3A%22u1f6f97ad%22%2C%22card%22%3A%22file%22%7D)


**前言**
如果用户需要同时流计算、批处理的场景下，用户需要维护两套业务代码，开发人员也要维护两套技术栈，非常不方便。
Flink 社区很早就设想过将批数据看作一个有界流数据，将批处理看作流计算的一个特例，从而实现流批统一，Flink 社区的开发人员在多轮讨论后，基本敲定了Flink 未来的技术架构：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636784184078-7780fc64-e842-4e97-9c61-e25b686a78c5.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=183&id=u526d7f9d&margin=%5Bobject%20Object%5D&name=image.png&originHeight=183&originWidth=399&originalType=binary&ratio=1&rotation=0&showTitle=false&size=24083&status=done&style=none&taskId=u45f0acd5-bcda-4834-b772-bf2e34e1d30&title=&width=399)
**Apache Flink 有两种关系型 API 来做流批统一处理：Table API 和 SQL**。
Table API 是用于 Scala 和 Java 语言的查询API，它可以用一种非常直观的方式来组合使用选取、过滤、join 等关系型算子。
Flink SQL 是基于 [Apache Calcite](https://calcite.apache.org/) 来实现的标准 SQL。这两种 API 中的查询对于批（DataSet）和流（DataStream）的输入有相同的语义，也会产生同样的计算结果。

# 一、核心概念
Flink 的[Table API](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/tableApi.html)和[SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/sql/)是流批统一的 API。这意味着TableAPI & SQL在无论有限的批式输入还是无限的流式输入下，都具有相同的语义。因为传统的关系代数以及 SQL 最开始都是为了批式处理而设计的，关系型查询在流式场景下不如在批式场景下容易理解。
## 1.1 动态表和连续查询
**_动态表(Dynamic Tables)_**是Flink的支持流数据的Table API和SQL的核心概念。与表示批处理数据的静态表不同，动态表是**随时间变化**的。可以像查询静态批处理表一样查询它们。查询动态表将生成一个**_连续查询(Continuous Query)_**。一个连续查询永远不会终止，结果会生成一个动态表。查询不断更新其(动态)结果表，以反映其(动态)输入表上的更改。
需要注意的是，连续查询的结果在语义上总是等价于以批处理模式在输入表快照上执行的相同查询的结果。
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636784793471-b82fdeeb-d84d-4ce7-8f20-68350da64ebf.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&id=ub95d4541&margin=%5Bobject%20Object%5D&name=image.png&originHeight=63&originWidth=415&originalType=binary&ratio=1&rotation=0&showTitle=false&size=35179&status=done&style=none&taskId=u59cf2dcd-76ed-41be-a8d7-a1b5ff4abca&title=)

1. 将流转换为动态表。
1. 在动态表上计算一个连续查询，生成一个新的动态表。
1. 生成的动态表被转换回流。
### 在流上定义表(动态表)
为了使用关系查询处理流，必须将其转换成Table。从概念上讲，流的每条记录都被解释为对**结果表的 INSERT**操作。
假设有如下格式的数据:
```yaml
[
  user:  VARCHAR,   // 用户名
  cTime: TIMESTAMP, // 访问 URL 的时间
  url:   VARCHAR    // 用户访问的 URL
]
```
下图显示了单击事件流(左侧)如何转换为表(右侧)。当插入更多的单击流记录时，结果表的数据将**不断增长**。
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636785257117-b1b08a0a-2a1c-48dd-8205-e5f79f95800b.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=152&id=ud0b7d442&margin=%5Bobject%20Object%5D&name=image.png&originHeight=152&originWidth=415&originalType=binary&ratio=1&rotation=0&showTitle=false&size=64550&status=done&style=none&taskId=u8e147586-abfb-402a-ab07-6b2adc49593&title=&width=415)
### 连续查询
在动态表上计算一个连续查询，并生成一个新的动态表。与批处理查询不同，连续查询从不终止，并根据**其输入表上的更新更新其结果表**。
在任何时候，连续查询的结果在语义上与以批处理模式在输入表快照上执行的相同查询的结果相同。
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636785366872-40b702d4-3ca7-466a-b644-ba04df762f63.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=209&id=u59fd8213&margin=%5Bobject%20Object%5D&name=image.png&originHeight=209&originWidth=415&originalType=binary&ratio=1&rotation=0&showTitle=false&size=26328&status=done&style=none&taskId=ub2a19330-fbe7-4bb4-9077-52ff0802653&title=&width=415)

1. 当查询开始，clicks 表(左侧)是空的。
1. 当**第一行数据**被插入到 clicks 表时，查询开始计算结果表。第一行数据 [Mary,./home] 插入后，结果表(右侧，上部)由一行 [Mary, 1] 组成。
1. 当第二行 [Bob, ./cart] 插入到 clicks 表时，查询会更新结果表并插入了一行新数据 [Bob, 1]。
1. 第三行 [Mary, ./prod?id=1] 将产生已计算的结果行的更新，[Mary, 1] 更新成 [Mary, 2]。
1. 最后，当第四行数据加入 clicks 表时，查询将第三行 [Liz, 1] 插入到结果表中。



#  二、Flink Table Api
## 2.1 maven依赖
```xml
<properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <flink.version>1.12.0</flink.version>
        <java.version>1.8</java.version>
        <scala.binary.version>2.11</scala.binary.version>
        <slf4j.version>1.7.30</slf4j.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-to-slf4j</artifactId>
            <version>2.14.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner-blink_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-csv</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>RELEASE</version>
            <scope>compile</scope>
        </dependency>
    </dependencies>
```
2、log4j.properties
```bash
log4j.rootLogger=error, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n
```
3、WaterSensor
```java


/**
 * 水位传感器：用于接收水位数据
 *
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
```
## 2.2 基本使用
### 表与DataStream的混合使用
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636788546950-c616f649-6ec2-4be9-8314-1f00264c8775.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=63&id=u43ebfc76&margin=%5Bobject%20Object%5D&name=image.png&originHeight=63&originWidth=415&originalType=binary&ratio=1&rotation=0&showTitle=false&size=35179&status=done&style=none&taskId=u14369438-ee4c-491e-ae3a-269ac6cab76&title=&width=415)
案例：筛选出指定id的水位传感器
```java
/**
 * @author long
 * @description: flink table api测试
 * @date 2021/11/7 21:06
 */
public class Flink01_TableApi_BasicUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取
        Table table = tableEnv.fromDataStream(waterSensorStream);
        // 3. 对动态表进行查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));
        // 4. 把动态表转换成流
        DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);
        resultStream.print();

        env.execute();
    }
}
```
结果
```yaml
sensor_1,1000,10
sensor_1,2000,20
sensor_1,4000,40
sensor_1,5000,50
```
### 聚合操作
案例：计算水位之和
```java
public class Flink01_TableApi_BasicUseAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 创建表: 将流转换成动态表. 表的字段名从pojo的属性名自动抽取
        Table table = tableEnv.fromDataStream(waterSensorStream);
        // 3. 对动态表进行查询
        Table resultTable = table
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"), $("vc_sum"));
        // 4. 把动态表转换成流 如果涉及到数据的更新, 要用到撤回流. 多个了一个boolean标记
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
        resultStream.print();

        env.execute();
    }
}
```
结果：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636788910880-b8106319-acb2-4903-a70c-ece76e689aa4.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=348&id=u1fef67d7&margin=%5Bobject%20Object%5D&name=image.png&originHeight=348&originWidth=949&originalType=binary&ratio=1&rotation=0&showTitle=false&size=49405&status=done&style=none&taskId=u831849d8-1396-466e-8e0c-596c0fe739c&title=&width=949)
## 2.3 表到流的转换
动态表可以像普通数据库表一样通过INSERT、UPDATE和DELETE来不断修改。它可能是一个只有一行、不断更新的表，也可能是一个insert-only的表，没有UPDATE 和 DELETE修改，或者介于两者之间的其他表。
在将动态表转换为流或将其写入外部系统时，需要对这些更改进行编码。Flink的 Table API和SQL支持三种方式来编码一个动态表的变化:
### Append-only 流
仅通过** INSERT **操作修改的动态表可以通过输出插入的行转换为流。
### Retract 流
retract 流包含两种类型的 message： _add messages_ 和 _retract messages_ 。通过**将INSERT 操作编码为 add message、将 DELETE 操作编码为 retract message、将 UPDATE 操作编码为更新(先前)行的 retract message 和更新(新)行的 add message，将动态表转换为 retract 流**。下图显示了将动态表转换为 retract 流的过程。
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636789714471-c9d769a3-df7f-4098-b625-3a846dc971a4.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=154&id=u7e23b8e6&margin=%5Bobject%20Object%5D&name=image.png&originHeight=154&originWidth=415&originalType=binary&ratio=1&rotation=0&showTitle=false&size=41586&status=done&style=none&taskId=u82cb6f5c-c504-4214-85b3-7b7d738bac0&title=&width=415)
【说明】
可以理解为：更新操作是先删除后新增。
Upsert 流
upsert 流包含两种类型的 message： _**upsert messages**_ 和** **_**delete messages**_。**转换为 upsert 流的动态表需要(可能是组合的)唯一键**。通过将 INSERT 和 UPDATE 操作编码为 upsert message，将 DELETE 操作编码为 delete message ，将具有唯一键的动态表转换为流。消费流的算子需要知道唯一键的属性，以便正确地应用 message。与 retract 流的主要区别在于 UPDATE 操作是用单个 message 编码的，因此效率更高。下图显示了将动态表转换为 upsert 流的过程。
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636790067895-5113aa03-8fd5-40c3-889f-63f0cf8debe7.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=163&id=uf10819eb&margin=%5Bobject%20Object%5D&name=image.png&originHeight=163&originWidth=415&originalType=binary&ratio=1&rotation=0&showTitle=false&size=25900&status=done&style=none&taskId=u9fb2e3de-31de-409b-9ad6-d78b7b5084d&title=&width=415)
**请注意，在将动态表转换为 DataStream 时，只支持 append 流和 retract 流。**
## 2.4 Source: 通过Connector声明读入数据
前面是先得到流, 再转成动态表, 其实动态表也可以直接连接到数据.
### File Source
案例：wordCount
```java
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wuxiaolong10
 * @description：
 * @since 13.11.21 3:56 下午
 */
public class Flink02_TableApi_Connector_FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2.1 表的元数据信息
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        // 2.2 连接文件, 并创建一个临时表, 其实就是一个动态表
        tableEnv.connect(new FileSystem().path("/Users/wuxiaolong10/IdeaProjects/bigdata-ware/flink-action/src/main/resources/files/sensor.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");
        // 3. 做成表对象, 然后对动态表进行查询
        Table sensorTable = tableEnv.from("sensor");
        Table resultTable = sensorTable
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));
        // 4. 把动态表转换成流. 如果涉及到数据的更新, 要用到撤回流. 多个了一个boolean标记
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
        resultStream.print();

        env.execute();
    }
}
```
sensor.txt
```yaml
sensor_1,2000,20
sensor_2,3000,30
sensor_1,4000,40
sensor_1,5000,50
sensor_2,6000,60
```
结果:
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636792269732-c1985795-5db4-423e-a3d4-c131783533d7.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=353&id=ub1e09517&margin=%5Bobject%20Object%5D&name=image.png&originHeight=353&originWidth=972&originalType=binary&ratio=1&rotation=0&showTitle=false&size=48019&status=done&style=none&taskId=u3cfbe979-c977-4661-8353-082038f369c&title=&width=972)
### Kafka Source
**1、准备工作：安装kafka**
a. 已部署好zookeeper并启动
b. 部署kafka
```shell
docker pull wurstmeister/kafka:2.12-2.2.2

# 启动kafka
docker run -d --name kafka --publish 9092:9092 \
--link zookeeper \
--env KAFKA_ZOOKEEPER_CONNECT=192.168.56.200:2181 \
--env KAFKA_ADVERTISED_HOST_NAME=192.168.56.200 \
--env KAFKA_ADVERTISED_PORT=9092  \
--volume /etc/localtime:/etc/localtime \
wurstmeister/kafka:2.12-2.2.2

sudo docker update kafka --restart=always
```
c. kafka常用命令
```bash
# 进入容器bash
docker exec -it kafka bash

# 进入kafka下bin目录
cd /opt/kafka_2.13-2.7.0

# 创建一个kafka topic，记得修改zk机IP,结果 Created topic test01.
bin/kafka-topics.sh --create --zookeeper 192.168.56.200:2181 --replication-factor 1 --partitions 1 --topic mykafka

# 查看topic
bin/kafka-topics.sh --list --zookeeper 192.168.56.200:2181

# 创建生产者
bin/kafka-console-producer.sh --broker-list 192.168.56.200:9092 --topic mykafka 

# 创建消费者
bin/kafka-console-consumer.sh --bootstrap-server 192.168.56.200:9092 --topic mykafka --from-beginning
```
2、案例实现
```java
public class Flink04_TableApi_Connector_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.1 表的元数据信息
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .startFromLatest()
                .property("group.id", "bigdata")
                .property("bootstrap.servers", "192.168.1.101:9092"))
        .withFormat(new Csv()
        )
        .withSchema(schema)
        .createTemporaryTable("sensor");

        // 3. 对动态表进行查询
        Table sensorTable = tableEnv.from("sensor");
        Table resultTable = sensorTable
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        // 4. 把动态表转换成流. 如果涉及到数据的更新, 要用到撤回流. 多个了一个boolean标记
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
        resultStream.print();

        env.execute();
    }
}
```
3、结果验证
（1）kafka生产者启动
```bash
# bin/kafka-console-producer.sh --broker-list 192.168.1.101:9092 --topic sensor
>sensor_1,2000,20
>sensor_2,3000,30
```
程序结果输出：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636817122157-c728c080-6bab-4906-acf3-cb0d8fa87897.png#clientId=u2740b51a-966e-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=189&id=u4663803f&margin=%5Bobject%20Object%5D&name=image.png&originHeight=189&originWidth=979&originalType=binary&ratio=1&rotation=0&showTitle=false&size=33356&status=done&style=none&taskId=ue189ce5c-759b-46a5-8b31-b32684bab61&title=&width=979)


## 2.5 Sink: 通过Connector声明写出数据
### File Sink
案例说明：把结果写入到文件中

- 参考代码
```java
import com.betop.action.entity.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wuxiaolong10
 * @description：
 * @since 14.11.21 10:29 上午
 */
public class Flink02_TableApi_Connector_FileSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table sensorTable = tableEnv.fromDataStream(waterSensorStream);
        Table resultTable = sensorTable
                .where($("id").isEqual("sensor_1") )
                .select($("id"), $("ts"), $("vc"));
        // 创建输出表
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        tableEnv.connect(new FileSystem().path("/Users/wuxiaolong10/IdeaProjects/bigdata-ware/flink-action/src/main/resources/output/sensor.txt"))
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 把数据写入到输出表中
        resultTable.executeInsert("sensor");
    }
}
```

- 运行结果: 在指定的目录生成文件sensor.txt

![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636857388409-3ee525cf-6af6-43d3-b583-a276c10db4b2.png#clientId=u2740b51a-966e-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=157&id=u4f9712f1&margin=%5Bobject%20Object%5D&name=image.png&originHeight=157&originWidth=479&originalType=binary&ratio=1&rotation=0&showTitle=false&size=10446&status=done&style=none&taskId=uc1bab070-5b21-4bcb-a014-570b60d8084&title=&width=479)




### kafka Sink
案例说明：把数据写入kafka
```java
/**
 * @author wuxiaolong10
 * @description：
 * @since 14.11.21 10:37 上午
 */
public class Flink03_TableApi_Connector_KafkaSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table sensorTable = tableEnv.fromDataStream(waterSensorStream);
        Table resultTable = sensorTable
                .where($("id").isEqual("sensor_1") )
                .select($("id"), $("ts"), $("vc"));
        // 创建输出表
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .startFromLatest()
                        .sinkPartitionerRoundRobin()
                        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:9092"))
                .withFormat(new Json()) // new Csv() 也是可以的
                .withSchema(schema)
                .createTemporaryTable("sensor");

        // 把数据写入到输出表中
        resultTable.executeInsert("sensor");
    }
}
```
运行结果：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636859531042-482fca2f-f511-46d7-b8d3-fce3a3bf0d62.png#clientId=u2740b51a-966e-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=74&id=u516d61a0&margin=%5Bobject%20Object%5D&name=image.png&originHeight=74&originWidth=781&originalType=binary&ratio=1&rotation=0&showTitle=false&size=34301&status=done&style=none&taskId=u4cd71946-1d06-4f30-9c54-e9fac569479&title=&width=781)
# 三、Flink SQL
## 3.1 基本使用
### 查询未注册的表
```java
import com.betop.action.entity.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wuxiaolong10
 * @description：
 * @since 13.11.21 4:33 下午
 */
public class Flink05_SQL_BaseUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 使用sql查询未注册的表
        Table inputTable = tableEnv.fromDataStream(waterSensorStream);
        Table resultTable = tableEnv.sqlQuery("select * from " + inputTable + " where id='sensor_1'");
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();
    }
}
```
结果：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636792634930-bbb99d4b-3fd6-4509-a5e1-2413f8682336.png#clientId=uab77623f-3e26-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=254&id=u0421c2db&margin=%5Bobject%20Object%5D&name=image.png&originHeight=254&originWidth=964&originalType=binary&ratio=1&rotation=0&showTitle=false&size=36858&status=done&style=none&taskId=ua534c713-6cca-45ee-87c2-b78e51d2592&title=&width=964)
### 查询已注册的表
```java
public class Flink06_SQL_BaseUse_RegisteredTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 使用sql查询未注册的表
        Table inputTable = tableEnv.fromDataStream(waterSensorStream);
        // 2. 把注册为一个临时视图
        tableEnv.createTemporaryView("sensor", inputTable);
        // 3. 在临时视图查询数据, 并得到一个新表
        Table resultTable = tableEnv.sqlQuery("select * from sensor where id='sensor_1'");
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();
    }
}
```
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636859792723-8218e97f-245c-4891-868a-0f4b140bae29.png#clientId=u2740b51a-966e-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=248&id=ua52ff624&margin=%5Bobject%20Object%5D&name=image.png&originHeight=248&originWidth=992&originalType=binary&ratio=1&rotation=0&showTitle=false&size=37402&status=done&style=none&taskId=u95c26565-e9c8-4d2d-8a9b-d7404b33c24&title=&width=992)
## 3.2 kafka到kafka
maven依赖
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka_2.11</artifactId>
  <version>1.14.0</version>
</dependency>
```
使用sql从**Kafka读数据**, 并**写入到Kafka**中。
```java
public class Flink_SQL_Kafka2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 注册SourceTable: source_sensor
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sql_source_sensor',"
                + "'properties.bootstrap.servers' = '192.168.1.101:9092',"
                + "'properties.group.id' = 'atguigu',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'csv'"
                + ")");

        // 2. 注册SinkTable: sink_sensor
        tableEnv.executeSql("create table sink_sensor(id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sql_sink_sensor',"
                + "'properties.bootstrap.servers' = '192.168.1.101:9092',"
                + "'format' = 'csv'"
                + ")");

        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");
    }
}
```


程序结果：
（1）启动生产者
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636874318359-de1bf849-b7ce-40ab-bb12-1872e2b9d217.png#clientId=u2740b51a-966e-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=78&id=u5f7c58e0&margin=%5Bobject%20Object%5D&name=image.png&originHeight=78&originWidth=850&originalType=binary&ratio=1&rotation=0&showTitle=false&size=31075&status=done&style=none&taskId=u40ac9c55-5ceb-450a-886e-fd2b2fa5913&title=&width=850)
（2）启动消费者
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636874367048-6901426f-d9c6-4d6e-ae25-66e00606cfbd.png#clientId=u2740b51a-966e-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=60&id=uf6fa2172&margin=%5Bobject%20Object%5D&name=image.png&originHeight=60&originWidth=867&originalType=binary&ratio=1&rotation=0&showTitle=false&size=24553&status=done&style=none&taskId=uf652c2e8-2f6b-4674-b7bb-9b89ae10a88&title=&width=867)
# 四、时间属性
像窗口（在 Table API 和 SQL ）这种基于时间的操作，需要有时间信息。因此，Table API 中的表就需要提供逻辑时间属性来表示时间，以及支持时间相关的操作。
## 4.1 处理时间
### DataStream 到 Table 转换时定义
处理时间属性可以在schema定义的时候用.proctime后缀来定义。时间属性一定**不能定义在一个已有字段上**，所以它**只能定义在schema定义的最后。**


案例：
```java
public class Flink_SQL_Time_ProcessTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.readTextFile("file/sensor.txt")
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        table.printSchema();
    }
}
```
运行结果：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1636875727181-a1e337a7-1889-4c34-aaca-1ce4c0a90e7c.png#clientId=u2740b51a-966e-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=266&id=ud4b29e1a&margin=%5Bobject%20Object%5D&name=image.png&originHeight=266&originWidth=788&originalType=binary&ratio=1&rotation=0&showTitle=false&size=22790&status=done&style=none&taskId=uce39253e-85bc-4e43-8491-ca1b68251d3&title=&width=788)
### 在创建表的 DDL 中定义
案例
```java
public class Flink_SQL_Time_ProcessTimeDDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 创建表, 声明一个额外的列作为处理时间
        tableEnv.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as PROCTIME()) with("
                + "'connector' = 'filesystem',"
                + "'path' = '/Users/wuxiaolong10/IdeaProjects/bigdata-ware/flink-action/src/main/resources/files/sensor.txt',"
                + "'format' = 'csv'"
                + ")");

        // 将DDL创建的表生成动态表
        Table sensor = tableEnv.from("sensor");
        // 打印元数据信息
        sensor.printSchema();

        // 执行sql
        TableResult result = tableEnv.executeSql("select * from sensor");
        result.print();
    }
}
```
执行结果：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1638587911705-ed02e635-c729-417c-9d65-47af6f3e2353.png#clientId=u5b041e33-2ff5-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=257&id=uf7df500a&margin=%5Bobject%20Object%5D&name=image.png&originHeight=514&originWidth=1080&originalType=binary&ratio=1&rotation=0&showTitle=false&size=61071&status=done&style=none&taskId=u49c2f165-3f7f-478c-b963-c496da83a53&title=&width=540)
## 4.2 事件时间
**事件时间允许程序按照数据中包含的时间来处理，这样可以在有乱序或者晚到的数据的情况下产生一致的处理结果**。它可以保证从外部存储读取数据后产生可以复现（replayable）的结果。
除此之外，**事件时间可以让程序在流式和批式作业中使用同样的语法**。在流式程序中的事件时间属性，在批式程序中就是一个正常的时间字段。
为了能够处理乱序的事件，并且区分正常到达和晚到的事件，Flink 需要从事件中获取事件时间并且产生 watermark（[watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_time.html)）。
​

### DataStream 到 Table 转换时定义
事件时间属性可以用 **.rowtime** 后缀在定义DataStream schema 的时候来定义。[时间戳和watermark](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_time.html)在这之前一定是**在DataStream上已经定义好**了。
在从DataStream到Table转换时定义事件时间属性有两种方式。取决于用 .rowtime 后缀修饰的字段名字是否是已有字段，事件时间字段可以是：

      - 在 schema 的结尾追加一个新的字段。
      - 替换一个已经存在的字段。

不管在哪种情况下，事件时间字段都表示DataStream中定义的事件的时间戳。
```java
public class Flink_EventTime_2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 作为事件时间的字段必须是 timestamp 类型, 所以根据 long 类型的 ts 计算出来一个 t
        TableResult tableResult = tEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = '/Users/wuxiaolong10/IdeaProjects/bigdata-ware/flink-action/src/main/resources/files/sensor.txt',"
                + "'format' = 'csv'"
                + ")");

        // 注册表
        Table sensor = tEnv.from("sensor");
        // 打印与数据信息
        sensor.printSchema();

        tEnv.sqlQuery("select * from sensor").execute().print();
    }
}
```

### 在创建表的 DDL 中定义
事件时间属性可以用 WATERMARK 语句在 CREATE TABLE DDL 中进行定义。WATERMARK 语句在一个已有字段上定义一个 watermark 生成表达式，同时标记这个已有字段为时间属性字段。
```java
public class Flink_EventTime_2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 作为事件时间的字段必须是 timestamp 类型, 所以根据 long 类型的 ts 计算出来一个 t
        tEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = '/Users/wuxiaolong10/IdeaProjects/bigdata-ware/flink-action/src/main/resources/files/sensor.txt',"
                + "'format' = 'csv'"
                + ")");

        tEnv.sqlQuery("select * from sensor").execute().print();
    }
}
```
运行结果：
![image.png](https://cdn.nlark.com/yuque/0/2021/png/1157748/1638608974309-0b6d8977-9339-4c5c-a09a-f60fb1a535ff.png#clientId=u5b041e33-2ff5-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=269&id=ua6fa0a70&margin=%5Bobject%20Object%5D&name=image.png&originHeight=538&originWidth=1052&originalType=binary&ratio=1&rotation=0&showTitle=false&size=66623&status=done&style=none&taskId=u3f197a41-046a-41d4-9d48-23bf5b0b6cc&title=&width=526)
**说明:**
1.   把一个现有的列定义为一个为表标记事件时间的属性。该列的类型必须为 **TIMESTAMP(3)**，且是 schema 中的顶层列，它也可以是一个计算列。
2.   严格递增时间戳： WATERMARK FOR rowtime_column AS rowtime_column。
3.   递增时间戳： WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND。
4.   有界乱序时间戳： WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit。
