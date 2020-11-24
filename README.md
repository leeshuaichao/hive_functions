## 函数
#### 函数1: 正则匹配返回所有子串,并返回array
regexp_extract_all(字段: string, 正则: string, group: int),返回array: string,可用于一行转多行
类似于hutool的ReUtil.findAll()

#### 函数2: 待补充

## 打包并上传到服务器测试

#### 创建临时函数
```bash
# 添加jar包到当前窗口
add jar /home/hive/apache-hive-3.1.2/lib/hive_udf-1.0-SNAPSHOT.jar;
# 创建临时函数hutool工具类

#### 创建临时函数
```bash
# 添加jar包到当前窗口
add jar /home/hive/apache-hive-3.1.2/lib/hive_udf-1.0-SNAPSHOT.jar;
# 创建临时函数
create temporary function regexp_extract_all AS 'com.moxi.hive.udf.regexp.UdtfRegexpExtractAll';
```

#### 测试临时函数
```hiveql
select voice_num from (
select regexp_extract_all(ret.abc, "@#(.*?)#@", 1) as vn from (select "@#命中5#@我要承@#命中1#@@#命中2#@诺还款, 你@#命中3#@说我应该怎么办呢诺兰@#命中4#@" as abc) ret) test
LATERAL VIEW explode(test.vn) r as voice_num;
```

#### 删除临时函数
```hiveql
drop temporary function regexp_extract_all;
delete jar /home/hive/apache-hive-3.1.2/lib/hive_udf-1.0-SNAPSHOT.jar;
```

## 生成永久函数
#### 把jar包上传到hdfs
```bash
# 创建hdfs目录
hadoop fs -mkdir /lib
# jar添加到hdfs
hadoop fs -put /home/hive/apache-hive-3.1.2/lib/hive_udf-1.0-SNAPSHOT.jar /lib/
# 查看是否添加成功
hadoop fs -lsr /lib
```

#### 创建永久函数
```hiveql
create function data_mart.regexp_extract_all AS 'com.moxi.hive.udf.regexp.UdtfRegexpExtractAll' using jar 'hdfs:/lib/hive_udf-1.0-SNAPSHOT.jar';
create function data_center.regexp_extract_all AS 'com.moxi.hive.udf.regexp.UdtfRegexpExtractAll' using jar 'hdfs:/lib/hive_udf-1.0-SNAPSHOT.jar';
```

#### 测试
```hiveql
select voice_num from (
select regexp_extract_all(ret.abc, "@#(.*?)#@", 1) as vn from (select "@#命中5#@我要承@#命中1#@@#命中2#@诺还款, 你@#命中3#@说我应该怎么办呢诺兰@#命中4#@" as abc) ret) test
LATERAL VIEW explode(test.vn) r as voice_num;
```
