package ru.mai.lessons.rpks.impl.services;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Record;
import org.jooq.Result;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.repository.DataBaseReader;

import java.sql.SQLException;

@Slf4j
public class ServiceFiltering implements Service {
    private DataBaseReader dataBaseReader;

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

    private void readDataFromDatabase(){
        dataBaseReader.readRulesFromDB();
    }
    @Override
    public void start(Config config) {
        dataBaseReader = initOrGetExistingDBReader(config.getConfig("db"));
        try{
            if(dataBaseReader.connectToDataBase()){
                readDataFromDatabase();
            }
            else{
                log.error("There is a problem with database.");
            }
        }
        catch(SQLException exc){
            log.error("There is a problem with database.");
        }



    }
}
