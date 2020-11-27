package io.github.leeshuaichao.hive.udf.regexp;

import io.github.leeshuaichao.hive.udf.utils.RegexpUtils;
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
