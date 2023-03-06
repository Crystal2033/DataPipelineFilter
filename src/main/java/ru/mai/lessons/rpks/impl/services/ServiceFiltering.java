package ru.mai.lessons.rpks.impl.services;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
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
                    .build();
        }
        return dataBaseReader;
    }
    @Override
    public void start(Config config) {
        dataBaseReader = initOrGetExistingDBReader(config.getConfig("db"));
        try{
            if(dataBaseReader.connectToDataBase()){
                //TODO: implement in future
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
