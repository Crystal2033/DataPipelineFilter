package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DataBaseReader implements DbReader {

    private Config settings;
    private HikariDataSource dataSource;
    private HikariConfig config = new HikariConfig();

    public DataBaseReader(Config conf){
        settings = conf;
        connectToDb();
    }
    private void connectToDb(){
        config.setJdbcUrl(settings.getConfig("db").getString("jdbcUrl"));
        config.setUsername(settings.getConfig("db").getString("user"));
        config.setPassword(settings.getConfig("db").getString("password"));
        config.setDriverClassName(settings.getConfig("db").getString("driver"));
        dataSource = new HikariDataSource(config);
    }

    @Override
    public Rule[] readRulesFromDB() throws SQLException {
        try (Connection connection = dataSource.getConnection()){
            DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
            return dsl.select().from("filter_rules").fetchInto(Rule.class).toArray(Rule[]::new);
        }
    }
}
