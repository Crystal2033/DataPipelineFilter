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
    /**
     *
     * Мб, есть смысл вообще убрать dataBaseReader и сделать try() with resources
     */
    private DataBaseReader dataBaseReader;
    private final ConcurrentHashMap<String, Rule> rulesConcurrentMap = new ConcurrentHashMap<>();

    private DataBaseReader initOrGetExistingDBReader(Config configDB){
        if(dataBaseReader == null){
            dataBaseReader = DataBaseReader.builder()
                    .url(configDB.getString("jdbcUrl"))
                    .userName(configDB.getString("user"))
                    .password(configDB.getString("password"))
                    .driver(configDB.getString("driver"))
                    .additionalDBConfig(configDB.getConfig("additional_info"))
                    .build();
        }
        return dataBaseReader;
    }

    /**
     * rules is a NEW rules.
     * If something exists, it will be replaced.
     */

    @Override
    public void start(Config config) {
        try(ExecutorService threadPool = Executors.newFixedThreadPool(1)){
            dataBaseReader = initOrGetExistingDBReader(config.getConfig("db"));

            try{
                if(dataBaseReader.connectToDataBase()){
                    threadPool.submit(new RulesUpdaterThread(rulesConcurrentMap, dataBaseReader));
                }
                else{
                    log.error("There is a problem with database.");
                }
            }
            catch(SQLException exc){
                log.error("There is a problem with database.");
                threadPool.shutdown();
            }
        }

    }
}
