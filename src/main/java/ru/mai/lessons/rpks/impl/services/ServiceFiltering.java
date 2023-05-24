package ru.mai.lessons.rpks.impl.services;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.kafka.KafkaReaderImpl;
import ru.mai.lessons.rpks.impl.kafka.dispatchers.FilteringDispatcher;
import ru.mai.lessons.rpks.impl.repository.DataBaseReader;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceFiltering implements Service {
    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap = new ConcurrentHashMap<>();
    private Config outerConfig;
    public static final String TOPIC_NAME_PATH = "topic.name";
    public static final String KAFKA_NAME = "kafka";

    @Override
    public void start(Config config) {
        outerConfig = config;
        try (DataBaseReader dataBaseReader = initExistingDBReader(outerConfig.getConfig("db"))) {
            connectToDBAndWork(dataBaseReader);
        }
    }

    private void startKafkaReader(FilteringDispatcher dispatcherKafka) {

        Config config = outerConfig.getConfig(KAFKA_NAME).getConfig("consumer");

        KafkaReaderImpl kafkaReader = KafkaReaderImpl.builder()
                .topic(config.getConfig("filtering").getString(TOPIC_NAME_PATH))
                .autoOffsetReset(config.getString(("auto.offset.reset")))
                .bootstrapServers(config.getString("bootstrap.servers"))
                .groupId(config.getString("group.id"))
                .dispatcherKafka(dispatcherKafka)
                .build();

        kafkaReader.processing();
    }

    private DataBaseReader initExistingDBReader(Config configDB) {
        return DataBaseReader.builder()
                .url(configDB.getString("jdbcUrl"))
                .userName(configDB.getString("user"))
                .password(configDB.getString("password"))
                .driver(configDB.getString("driver"))
                .additionalDBConfig(configDB.getConfig("additional_info"))
                .build();
    }

    private void connectToDBAndWork(DataBaseReader dataBaseReader) {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        connectAndRun(dataBaseReader, scheduledExecutorService);
        scheduledExecutorService.shutdown();
    }

    private void connectAndRun(DataBaseReader dataBaseReader, ScheduledExecutorService executorService) {
        try {
            if (dataBaseReader.connectToDataBase()) {

                RulesUpdaterThread rulesDBUpdaterThread = new RulesUpdaterThread(rulesConcurrentMap, dataBaseReader);

                Config config = outerConfig.getConfig(KAFKA_NAME)
                        .getConfig("producer");
                FilteringDispatcher filterDispatcher = new FilteringDispatcher(config.getConfig("deduplication")
                        .getString(TOPIC_NAME_PATH), config.getString("bootstrap.servers"), rulesDBUpdaterThread);

                long delayTimeInSec = outerConfig.getConfig("application").getLong("updateIntervalSec");
                executorService.scheduleWithFixedDelay(rulesDBUpdaterThread, 0, delayTimeInSec, TimeUnit.SECONDS);

                startKafkaReader(filterDispatcher);
            } else {
                log.error("There is a problem with connection to database.");
            }
        } catch (SQLException exc) {
            log.error("There is a problem with getConnection from Hikari.");
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        } finally {
            log.info("All threads are done.");
            executorService.shutdownNow();
        }
    }
}
