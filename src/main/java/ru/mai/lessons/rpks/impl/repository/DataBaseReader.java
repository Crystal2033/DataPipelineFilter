package ru.mai.lessons.rpks.impl.repository;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Builder;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

@Builder
public class DataBaseReader implements DbReader {
    private final String url;
    private final String userName;
    private final String password;

    private final String driver;

    private final HikariConfig config = new HikariConfig();
    private HikariDataSource dataSource;

    private Connection dataSourceConnection;

    private void initHikariConfig(){
        config.setJdbcUrl(url);
        config.setUsername(userName);
        config.setPassword(password);
        config.setDriverClassName(driver);
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
    }

    public boolean connectToDataBase() throws SQLException {
        initHikariConfig();
        if(dataSource == null){
            dataSource = new HikariDataSource(config);
        }
        dataSourceConnection = dataSource.getConnection();
        return dataSourceConnection.isValid(1000);
    }

    @Override
    public Rule[] readRulesFromDB() {

        return new Rule[0];
    }
}
