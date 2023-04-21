package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static ru.mai.lessons.rpks.impl.Db.getConnection;
@Slf4j
public class ServiceFiltering implements Service {
    Db db;
    Rule[] rules;
    int updateIntervalSec;
    ConcurrentLinkedQueue<Message> queue;

    @Override
    public void start(Config config) {
        rules = new Rule[1];
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        ConfigurationReader configurationReader = new ConfigurationReader();
        updateIntervalSec = configurationReader.loadConfig().getInt("application.updateIntervalSec");
        ArrayList<ArrayList<String>> arrayLists = new ArrayList<>();
        queue = new ConcurrentLinkedQueue<>();
        db = new Db();
        Connection conn = null;
        try {
            conn = getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        {
            DSLContext context = DSL.using(conn, SQLDialect.POSTGRES);
            rules = db.readRulesFromDB(context);

            updateIntervalSec = config.getConfig("application").getInt("updateIntervalSec");
            TimerTask task = new TimerTask() {
                public void run() {
                    log.info("asdfghj");
                    rules = db.readRulesFromDB(context);
                    for (Rule r :
                            rules) {
                        log.info(r.toString());

                    }
                }
            };

            Timer timer = new Timer(true);
            timer.schedule(task, 0, 1000 * updateIntervalSec);
            log.info("delay:" + updateIntervalSec);


//        String query = "SELECT * FROM testRules";
            //Using try-with-resources for auto closing connection, pstmt, and rs.
//        try (Connection connection = getConnection();
//             PreparedStatement pstmt = connection.prepareStatement(query);
//             ResultSet rs = pstmt.executeQuery();
//        ) {
//            if (rs.next()) {
//                System.out.println("Result is " + rs.getInt(1) + rs.getString(2));
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        List<NewTopic> topics = Stream.of("test_topic_in", "test_topic_out")
//                .map(topicName -> new NewTopic(topicName, 3, (short) 1))
//                .toList();
//
//        log.info("Topics: {}, replica factor {}, partitions {}", topics, (short)1, 3);
//        ObjectMapper mapper = new ObjectMapper();
//        Map<String, Object> map;
//        String json = "{\"name\":\"alex\", \"age\":18, \"sex\":\"M\"}";
//
////        Person user = new Person("");
//
//// To put all of the JSON in a Map<String, Object>
//        try {
//            map = mapper.readValue(json, Map.class);
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
////        }
//
//// Accessing the three target data elements
//        Map<String, Object> preDateMap = (Map) map.get("pre-date");
//        System.out.println(preDateMap.get("enable"));
//        System.out.println(preDateMap.get("days"));
//        System.out.println(preDateMap.get("interval"));
            String reader = configurationReader.loadConfig().getString("kafka.consumer.bootstrap.servers");
            String writer = configurationReader.loadConfig().getString("kafka.producer.bootstrap.servers");
            KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic_in", "test_topic_out", reader, rules);
            executorService.execute(() -> {
//            KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic", reader, rules);
                queue = kafkaReader.getQueue();


//            queue = kafkaReader.getQueue();
                System.out.println("+++++++" + queue);
            });
            KafkaWriterImpl kafkaWriter = new KafkaWriterImpl("test_topic_in", writer);
//        KafkaQueueWriter kafkaQueueWriter = new KafkaQueueWriter("test_topic_out", writer, queue);

            executorService.execute(() -> {
//            KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic", reader, rules);
//            log.info("ya v potoke");
//            queue = kafkaReader.getQueue();
                kafkaReader.setRules(rules);
                kafkaReader.processing();
            });


//            queue = kafkaReader.getQueue();
//            System.out.println("+++++++"+queue);

            System.out.println("queue" + queue);
            kafkaWriter.processing();
////        kafkaWriter.setQueue(queue);
//        System.out.println("-------------");
//
//        executorService.execute(() -> {
////            KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic", reader, rules);
//            kafkaWriter.processing();
//        });
//        executorService.execute(() -> {
////            KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic", reader, rules);
//            kafkaQueueWriter.processing();
//        });


            // написать код реализации сервиса фильтрации
        }
    }
}