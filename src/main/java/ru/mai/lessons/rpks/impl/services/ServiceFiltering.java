package ru.mai.lessons.rpks.impl.services;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.constants.MainNames;
import ru.mai.lessons.rpks.impl.kafka.KafkaReaderImpl;
import ru.mai.lessons.rpks.impl.kafka.KafkaWriterImpl;
import ru.mai.lessons.rpks.impl.repository.DataBaseReader;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ServiceFiltering implements Service {
    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap = new ConcurrentHashMap<>();

    private DataBaseReader initExistingDBReader(Config configDB){
        return DataBaseReader.builder()
                .url(configDB.getString("jdbcUrl"))
                .userName(configDB.getString("user"))
                .password(configDB.getString("password"))
                .driver(configDB.getString("driver"))
                .additionalDBConfig(configDB.getConfig("additional_info"))
                .build();
    }

    private void connectToDBAndWork(DataBaseReader dataBaseReader){
        try{
            if(dataBaseReader.connectToDataBase()){
                RulesUpdaterThread rulesDBUpdaterThread = new RulesUpdaterThread(rulesConcurrentMap, dataBaseReader);
                CompletableFuture<?> readDBFuture = CompletableFuture.runAsync(rulesDBUpdaterThread);

                CompletableFuture<?> kafkaReaderFuture = CompletableFuture.runAsync(() -> {
                    Config config = ConfigFactory.load(MainNames.CONF_PATH)
                            .getConfig("kafka").getConfig("filtering").getConfig("consumer");

                    KafkaReaderImpl kafkaReader = KafkaReaderImpl.builder()
                            .topic(config.getString("topic.name"))
                            .autoOffsetReset(config.getString(("auto.offset.reset")))
                            .bootstrapServers(config.getString("bootstrap.servers"))
                            .groupId(config.getString("group.id"))
                            .build();

                    kafkaReader.processing();
                });

                CompletableFuture<?> kafkaWriterFuture = CompletableFuture.runAsync(() -> {
                    Config config = ConfigFactory.load(MainNames.CONF_PATH)
                            .getConfig("kafka");
                    String exitString = config.getString("exit.string");

                    config = config.getConfig("filtering").getConfig("producer");

                    KafkaWriterImpl kafkaWriter = KafkaWriterImpl.builder()
                            .bootstrapServers(config.getString("bootstrap.servers"))
                            .topic(config.getString("topic.name"))
                            .build();
                    kafkaWriter.initKafkaReader();

                    try (Scanner scanner = new Scanner(System.in)) {
                        String inputData;
                        do {
                            inputData = scanner.nextLine();
                            kafkaWriter.processing(Message.builder()
                                    .value(inputData)
                                    .build());

                        } while (!inputData.equals(exitString));
                    }
                });

                CompletableFuture.anyOf(kafkaReaderFuture, kafkaWriterFuture).join();
                if(kafkaReaderFuture.isDone() || kafkaWriterFuture.isDone()){
                    rulesDBUpdaterThread.stopReadingDataBase();
                }
            }
            else{
                log.error("There is a problem with connection to database.");
            }
        }
        catch(SQLException exc){
            log.error("There is a problem with getConnection from Hikari.");
        } finally {
            log.info("All threads are done.");
        }
    }

    @Override
    public void start(Config config) {
        try(DataBaseReader dataBaseReader = initExistingDBReader(config.getConfig("db")))
        {
            connectToDBAndWork(dataBaseReader);
        } catch (SQLException e) {
            log.error("There is a problem with initializing database.");
        }

    }
}
