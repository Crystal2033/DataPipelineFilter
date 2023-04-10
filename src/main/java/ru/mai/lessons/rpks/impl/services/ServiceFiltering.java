package ru.mai.lessons.rpks.impl.services;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.constants.MainNames;
import ru.mai.lessons.rpks.impl.kafka.KafkaReaderImpl;
import ru.mai.lessons.rpks.impl.kafka.KafkaWriterImpl;
import ru.mai.lessons.rpks.impl.kafka.dispatchers.DispatcherKafka;
import ru.mai.lessons.rpks.impl.kafka.dispatchers.FilteringDispatcher;
import ru.mai.lessons.rpks.impl.repository.DataBaseReader;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ServiceFiltering implements Service {
    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap = new ConcurrentHashMap<>();
    private Config myConfig;


    @Override
    public void start(Config config) {
        myConfig = config;
        try(DataBaseReader dataBaseReader = initExistingDBReader(myConfig.getConfig("db")))
        {
            connectToDBAndWork(dataBaseReader);
        } catch (SQLException e) {
            log.error("There is a problem with initializing database.");
        }
    }

    private void startKafkaReader(DispatcherKafka dispatcherKafka){

        Config config = myConfig.getConfig("kafka").getConfig("consumer");

        KafkaReaderImpl kafkaReader = KafkaReaderImpl.builder()
                .topic(config.getConfig("filtering").getString("topic.name"))
                .autoOffsetReset(config.getString(("auto.offset.reset")))
                .bootstrapServers(config.getString("bootstrap.servers"))
                .groupId(config.getString("group.id"))
                .dispatcherKafka(dispatcherKafka)
                .build();

        kafkaReader.processing();
    }

    private CompletableFuture<?> startKafkaWriter(){
        return CompletableFuture.runAsync(() -> {
            Config config = myConfig.getConfig("kafka");
            String exitString = config.getString("exit.string");

            config = config.getConfig("producer");

            KafkaWriterImpl kafkaWriter = KafkaWriterImpl.builder()
                    .bootstrapServers(config.getString("bootstrap.servers"))
                    .topic(config.getConfig("filtering").getString("topic.name"))
                    .build();
        });
    }
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
        ExecutorService executorService = Executors.newCachedThreadPool();
        try{
            if(dataBaseReader.connectToDataBase()){

                RulesUpdaterThread rulesDBUpdaterThread = new RulesUpdaterThread(rulesConcurrentMap, dataBaseReader, myConfig);

                Config config = myConfig.getConfig("kafka")
                        .getConfig("producer");
                DispatcherKafka filterDispatcher = new FilteringDispatcher(config.getConfig("deduplication")
                        .getString("topic.name"), config.getString("bootstrap.servers"), rulesDBUpdaterThread);


                executorService.execute(rulesDBUpdaterThread);

                startKafkaReader(filterDispatcher);
                executorService.shutdown();
                log.info("Daaaaaamn {}", Thread.currentThread().getName());
            }
            else{
                log.error("There is a problem with connection to database.");
            }
        }
        catch(SQLException exc){
            log.error("There is a problem with getConnection from Hikari.");
        }
        catch (Exception ex){
            log.error(ex.getMessage(), ex);
        }
        finally{
            log.info("All threads are done.");
        }
    }
}
