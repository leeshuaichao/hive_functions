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
