package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import ru.mai.lessons.rpks.ConfigReader;

import java.util.*;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        return ConfigFactory.parseResources("application.conf");
//        HashMap<String, String> map = new HashMap();
//        ArrayList<Map.Entry> lost = new ArrayList<Map.Entry>();
////        map.put("db.jdbcUrl", (config.getString("db.jdbcUrl")));
////        map.put("db.user", (config.getString("db.user")));
////        map.put("db.password", (config.getString("db.password")));
////        map.put("db.driver", (config.getString("db.driver")));
////
////        map.put("kafka.consumer.bootstrap.servers", (config.getString("kafka.consumer.bootstrap.servers")));
//        for (Map.Entry conf: config.entrySet()
//             ) {
//            int startingIndex;
//            int closingIndex;
//            if (conf.getValue().toString().contains("\"")) {
//                startingIndex = conf.getValue().toString().indexOf("(") + 1;
//                closingIndex = conf.getValue().toString().indexOf(")") - 1;
//            }
//            else {
//                startingIndex = conf.getValue().toString().indexOf("(");
//                closingIndex = conf.getValue().toString().indexOf(")");
//            }
////            String result1 = conf.getValue().toString().substring(startingIndex + 1, closingIndex);
////            conf.setValue(result1);
//            Map.Entry<String, String> conf2 = Map.entry(conf.getKey().toString(), conf.getValue().toString().substring(startingIndex + 1, closingIndex));
//            lost.add(conf2);
//            System.out.println(conf2);
//        }


         // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
    }
}
