package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;


public final class DbReaderImpl implements DbReader {
    private final Config config;
    private HikariDataSource ds;
    private final HikariConfig dataBaseConfig;
    private Connection connection;

    public DbReaderImpl(Config config)  {
        this.config = config;

        String url = config.getString("jdbcUrl");
        String user = config.getString("user");
        String password = config.getString("password");
        String driver = config.getString("driver");

        dataBaseConfig = new HikariConfig();
        dataBaseConfig.setJdbcUrl(url);
        dataBaseConfig.setPassword(password);
        dataBaseConfig.setDriverClassName(driver);
        dataBaseConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        dataBaseConfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        dataBaseConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );


        try {
            ds = new HikariDataSource(dataBaseConfig);
            connection = ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Rule[] readRulesFromDB() {

        return new Rule[0];
    }
}
