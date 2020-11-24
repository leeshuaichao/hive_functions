# 正则匹配返回所有子串
类似于hutool的ReUtil.findAll()

## 创建mvn项目
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.moxi.hive</groupId>
    <artifactId>hive_udf</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>HiveUDFs</name>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-exec</artifactId>
            <version>3.1.2</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

## 编写UDTF类
```java
package com.moxi.hive.udf.regexp;

import com.moxi.hive.udf.utils.RegexpUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * 正则匹配,返回匹配到的所有子串(返回regexp_extract的全部结果)
 * regexp_extract_all(字段, 正则, 返回第几个括号内的内容:0是全部)
 * @author lishuaichao@xi-ai.com
 * 2020/11/23 下午2:33
 **/
public class UdtfRegexpExtractAll extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // Check if two arguments were passed
        if (objectInspectors.length != 2 && objectInspectors.length != 3) {
            throw new UDFArgumentLengthException(
                    "The function regexp_extract_all takes exactly 2 or 3 arguments.");
        }

        for (int i = 0; i < 2; i++) {
            if (!ObjectInspectorUtils.compareTypes(PrimitiveObjectInspectorFactory.javaStringObjectInspector, objectInspectors[i])) {
                throw new UDFArgumentTypeException(i,
                        "\"" + PrimitiveObjectInspectorFactory.javaStringObjectInspector.getTypeName() + "\" "
                                + "expected at function regexp_extract_all, but "
                                + "\"" + objectInspectors[i].getTypeName() + "\" "
                                + "is found");
            }
        }

        if (objectInspectors.length == 3) {
            if (!ObjectInspectorUtils.compareTypes(PrimitiveObjectInspectorFactory.javaIntObjectInspector, objectInspectors[2])) {
                throw new UDFArgumentTypeException(2,
                        "\"" + PrimitiveObjectInspectorFactory.javaLongObjectInspector.getTypeName() + "\" "
                                + "expected at function regexp_extract_all, but "
                                + "\"" + objectInspectors[2].getTypeName() + "\" "
                                + "is found");
            }
        }

        ObjectInspector expect = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        return ObjectInspectorFactory.getStandardListObjectInspector(expect);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String source = deferredObjects[0].get().toString();
        String pattern = deferredObjects[1].get().toString();
        Integer groupIndex = 0;
        if (deferredObjects.length == 3) {
            groupIndex = ((IntWritable) deferredObjects[2].get()).get();
        }

        if (source == null) {
            return null;
        }

        return RegexpUtils.findAll(pattern, source, groupIndex);
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == 2 || strings.length == 3);
        if (strings.length == 2) {
            return "regexp_extract_all(" + strings[0] + ", "
                    + strings[1] + ")";
        } else {
            return "regexp_extract_all(" + strings[0] + ", "
                    + strings[1] + ", " + strings[2] + ")";
        }
    }

}
```

## 正则工具类
```java
package com.moxi.hive.udf.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则工具类
 * lishuaichao@xi-ai.com
 * 2020/11/23 下午2:45
 **/
public class RegexpUtils {
    /**
     * 查询所有子串并返回list
     * @param regex     正则表达式
     * @param content   被识别字符串
     * @param group     获取第几个括号的内容,0为整个正则
     * @return
     */
    public static List<String> findAll(String regex, CharSequence content, int group) {
        List<String> collection = new ArrayList<>();
        Pattern pattern = Pattern.compile(regex);
        if (null != content) {
            Matcher matcher = pattern.matcher(content);
            while(matcher.find()) {
                collection.add(matcher.group(group));
            }
        }
        return collection;
    }

}
```

## 打包并上传到服务器测试
#### 遇到的坑:
maven打jar包把依赖打进来需要特殊处理,因此未使用hutool工具类

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
