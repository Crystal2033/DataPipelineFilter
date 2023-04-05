package ru.mai.lessons.rpks.impl.services;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.kafka.KafkaReaderImpl;
import ru.mai.lessons.rpks.impl.kafka.KafkaWriterImpl;
import ru.mai.lessons.rpks.impl.repository.DataBaseReader;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.*;

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

    private void connectToDBAndWork(ExecutorService threadPool, DataBaseReader dataBaseReader){
        try{
            if(dataBaseReader.connectToDataBase()){
                Future<?> response = threadPool.submit(new RulesUpdaterThread(rulesConcurrentMap, dataBaseReader));
                response.get();

                ExecutorService executorService = Executors.newFixedThreadPool(1);
                executorService.execute(() -> {
                    Config config = ConfigFactory.load("application.conf").getConfig("consumer");
                    KafkaReaderImpl kafkaReader = KafkaReaderImpl.builder()
                            .topic(config.getString("topic.name"))
                            .autoOffsetReset(config.getString(("auto.offset.reset")))
                            .bootstrapServers(config.getString("bootstrap.servers"))
                            .groupId(config.getString("group.id"))
                            .build();

                    kafkaReader.processing();
                });

                Config config = ConfigFactory.load("application.conf").getConfig("producer");

                KafkaWriterImpl kafkaWriter = KafkaWriterImpl.builder()
                        .bootstrapServers(config.getString("bootstrap.servers"))
                        .topic(config.getString("topic.name"))
                        .build();
                kafkaWriter.initKafkaReader();

                try (Scanner scanner = new Scanner(System.in)) {
                    String inputData;
                    do {
                        inputData = scanner.nextLine();
                        String[] keyValue = inputData.split(":");

                        kafkaWriter.processing(Message.builder()
                                .value(keyValue[0])
                                .build());


                    } while (!inputData.equals("$exit"));

                    executorService.shutdown();
                }
                threadPool.shutdown();
            }
            else{
                log.error("There is a problem with connection to database.");
            }
        }
        catch(SQLException exc){
            log.error("There is a problem with getConnection from Hikari.");
            threadPool.shutdown();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start(Config config) {
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        try(DataBaseReader dataBaseReader = initExistingDBReader(config.getConfig("db")))
        {
            connectToDBAndWork(threadPool, dataBaseReader);
        } catch (SQLException e) {
            log.error("There is a problem with initializing database.");
        }

    }
}
