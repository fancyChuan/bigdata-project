package cn.fancychuan.qzpoint.util;


import com.alibaba.fastjson.JSONObject;

public class ParseJsonData {

    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }
}
