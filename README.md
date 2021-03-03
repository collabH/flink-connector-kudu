# Kudu Connector

* 基于Apache-Bahir-Kudu-Connector改造而来的满足公司内部使用的Kudu Connector，支持特性Range分区、定义Hash分桶数、支持Flink1.11.x动态数据源等，改造后已贡献部分功能给社区。

# 使用姿势

## 引入依赖

```xml
<dependency>
  	<artifactId>flink-connector-kudu_2.11</artifactId>
    <groupId>org.forchange.connector</groupId>
    <version>1.2-SNAPSHOT</version>
 </dependency>
```

## Kudu Catalog使用

### 创建Catalog

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
catalog = new KuduCatalog("cdh01:7051,cdh02:7051,cdh03:7051");
tableEnv = KuduTableTestUtils.createTableEnvWithBlinkPlannerStreamingMode(env);
tableEnv.registerCatalog("kudu", catalog);
tableEnv.useCatalog("kudu");
```

### Catalog API

```java
// dropTable
 catalog.dropTable(new ObjectPath("default_database", "test_Replice_kudu"), true);
 // 通过catalog操作表
tableEnv.sqlQuery("select * from test");
tableEnv.executeSql("drop table test");
tableEnv.executeSql("insert into testRange values(1,'hsm')");

```

## FlinkSQL

### KuduTable Properties

* 通过`connector.type`和`connector`区分使用`TableSourceFactory`还是`KuduDynamicTableSource`

```properties
kudu.table=指定映射的kudu表
kudu.masters=指定的kudu master地址
kudu.hash-columns=指定的表的hash分区键,多个使用","分割
kudu.replicas=kudu tablet副本数，默认为3
kudu.hash-partition-nums=hash分区的桶个数，默认为2 * replicas
kudu.range-partition-rule=range分区规则，rangeKey#leftValue,RightValue:rangeKey#leftValue1,RightValue1，rangeKey必须为主键
kudu.primary-key-columns=kudu表主键，多个实用","分割，主键定义必须有序
kudu.lookup.cache.max-rows=kudu时态表缓存最大缓存行，默认为不开启
kudu.lookup.cache.ttl=kudu时态表cache过期时间
kudu.lookup.max-retries=时态表join时报错重试次数，默认为3
```

#### Flink1.10.x版本

```java
CREATE TABLE TestTableTableSourceFactory (
  first STRING,
  second STRING,
  third INT NOT NULL
) WITH (
  'connector.type' = 'kudu',
  'kudu.masters' = '...',
  'kudu.table' = 'TestTable',
  'kudu.hash-columns' = 'first',
  'kudu.primary-key-columns' = 'first,second'
)
```

#### Flink1.11.x版本

```sql
CREATE TABLE TestTableKuduDynamicTableSource (
  first STRING,
  second STRING,
  third INT NOT NULL
) WITH (
  'connector' = 'kudu',
  'kudu.masters' = '...',
  'kudu.table' = 'TestTable',
  'kudu.hash-columns' = 'first',
  'kudu.primary-key-columns' = 'first,second'
)
```

## DataStream使用

* DataStream使用方式具体查看`bahir-flink`官方，目前对于数仓工程师使用场景偏少。

# 版本迭代

## 1.1版本Feature

* 增加Hash分区bucket属性配置,通过`kudu.hash-partition-nums`配置
* 增加Range分区规则,支持Hash和Range分区同时使用,通过参数`kudu.range-partition-rule`
  配置,规则格式如:`range分区规则，rangeKey#leftValue,RightValue:rangeKey#leftValue1,RightValue1`
* 增加Kudu时态表支持,通过`kudu.lookup.*`相关函数控制内存数据的大小和TTL

```java
 /**
     * lookup缓存最大行数
     */
  public static final String KUDU_LOOKUP_CACHE_MAX_ROWS = "kudu.lookup.cache.max-rows";
    /**
     * lookup缓存过期时间
     */
    public static final String KUDU_LOOKUP_CACHE_TTL = "kudu.lookup.cache.ttl";
    /**
     * kudu连接重试次数
     */
    public static final String KUDU_LOOKUP_MAX_RETRIES = "kudu.lookup.max-retries";
```

## 实现机制

* 自定义`KuduLookupFunction`,使得KuduTableSource实现`LookupableTableSource`接口将自定义`LookupFunction`
  返回已提供时态表的功能,底层缓存没有使用`Flink JDBC`的`Guava Cache`而是使用效率更高的`Caffeine Cache`使得其缓存效率更高,同时也减轻了因大量请求为Kudu带来的压力

## 未来展望

### 当前问题

1. SQL语句主键无法自动推断

> 目前基于`Apache Bahir Kudu Connector`增强的功能主要是为了服务公司业务,在使用该版本的connector也遇到了问题,SQL的主键无法自动推断导致数据无法直接传递到下游,内部通过天宫引擎通过`Flink Table API`的`sqlQuery`方法将结果集查询为一个`Table`对象,然后将`Table`转换为`DataStream<Tuple2<Boolean,Row>>`撤回流,最终通过`Kudu Connector`提供的`KuduSink`的`UpsertOperationMapper`对象将撤回流输出到`Kudu`中。

### 后续计划

* 计划提供动态数据源来解决这一问题,将`Flink 1.11.x`之前的`KuduTableSource/KuduTableSink`改造为`DynamicSource/Sink`接口实现`Source/Sink`,以此解决主键推断问题。

## 1.2版本Feature

* 改造支持`Flink 1.11.x`之后的`DynamicSource/Sink`，以此解决SQL语句主键无法推断问题，支持流批JOIN功能的SQL语句方式，无需在通过转换成DataStream的方式进行多表Join操作。
* 内嵌Metrics上报机制，通过对`Flink`动态工厂入口处对操作的kudu表进行指标埋点，从而更加可视化的监控kudu表数据上报问题。