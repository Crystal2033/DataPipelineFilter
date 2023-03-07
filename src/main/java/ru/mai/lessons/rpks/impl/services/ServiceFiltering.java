package ru.mai.lessons.rpks.impl.services;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Record;
import org.jooq.Result;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.repository.DataBaseReader;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ServiceFiltering implements Service {
    private final ConcurrentHashMap<String, Rule> rulesConcurrentMap = new ConcurrentHashMap<>();

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
                threadPool.submit(new RulesUpdaterThread(rulesConcurrentMap, dataBaseReader));
            }
            else{
                log.error("There is a problem with connection to database.");
            }
        }
        catch(SQLException exc){
            log.error("There is a problem with getConnection from Hikari.");
            threadPool.shutdown();
        }
    }

    @Override
    public void start(Config config) {
        try(ExecutorService threadPool = Executors.newFixedThreadPool(1);
            DataBaseReader dataBaseReader = initExistingDBReader(config.getConfig("db")))
        {
            connectToDBAndWork(threadPool, dataBaseReader);

        } catch (SQLException e) {
            log.error("There is a problem with initializing database.");
        }

    }
}
