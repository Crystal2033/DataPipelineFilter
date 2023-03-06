package ru.mai.lessons.rpks.impl.repository;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Record;
import org.jooq.*;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

import static org.jooq.impl.DSL.field;

@PlainSQL
@Slf4j
@Builder
public class DataBaseReader implements DbReader {
    private final String url;
    private final String userName;
    private final String password;
    private final String driver;

    private final Config additionalDBConfig;

    private final HikariConfig config = new HikariConfig();
    private HikariDataSource dataSource;

    private DSLContext dslContext;

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

    private void initDataSourceAndDSLContext(){
        dataSource = new HikariDataSource(config);
        dslContext = DSL.using(dataSource, SQLDialect.POSTGRES);
    }


    public boolean connectToDataBase() throws SQLException {
        initHikariConfig();
        if(dataSource == null){
            initDataSourceAndDSLContext();
        }
        dataSourceConnection = dataSource.getConnection();
        return dataSourceConnection.isValid(additionalDBConfig.getInt("connect_valid_time"));
    }

    @Override
    public Rule[] readRulesFromDB() {
        log.info(additionalDBConfig.getString("table_name"));
        Result<Record> queryResults = dslContext.select()
                .from(additionalDBConfig.getString("table_name"))
                .where(field("id").eq(additionalDBConfig.getInt("filter_id")))
                .fetch();


        return new Rule[0];
    }
}
