package com.alleyz.kafka.util;

import lombok.extern.log4j.Log4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by alleyz on 2017/4/27.
 *
 */
@Log4j
public class KafkaHelper {
    private KafkaHelper() {}

    private static Properties properties;
    static {
        try {
            properties = new Properties();
            properties.load(KafkaHelper.class.getResourceAsStream("/kafka.properties"));
        }catch (IOException e) {
            log.error("未找到配置文件[/kafka.properties]", e);
        }
    }
    public static String getProps(String key) {
        return properties.getProperty(key);
    }
    public static Integer getIntegerProps(String key) {
        String val = getProps(key);
        if(val == null || "".equals(val)) return null;
        return Integer.parseInt(val);
    }
    /**
     * 读取指定文件的配置
     * @param propsPath    文件路径
     * @return 配置
     * @throws IOException
     */
    public static Properties loadProps(String propsPath) throws IOException {
        InputStream is = null;
        try {
            is = KafkaHelper.class.getResourceAsStream(propsPath);
            Properties props = new Properties();
            props.load(is);
            return props;
        } finally {
            if (is != null) {
                try {
                    is.close();
                }catch (IOException e) {e.printStackTrace();}
            }
        }
    }

    public static Long getLongVal(String propKey, Properties props, Long defVal) {
        String propVal = props.getProperty(propKey);
        try {
            return Long.parseLong(propVal);
        }catch (Exception e) {
            return defVal;
        }
    }

}
