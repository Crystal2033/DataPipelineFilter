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
public class DbReaderImpl implements DbReader {
    Rule[] rules;
    String username;
    String password;
    String jdbcUrl;
    String driver;
    private static HikariConfig config;
    private static HikariDataSource ds;


    public DbReaderImpl(Config conf){
        username = conf.getConfig("db").getString("user");
        password = conf.getConfig("db").getString("password");
        jdbcUrl = conf.getConfig("db").getString("jdbcUrl");
        driver = conf.getConfig("db").getString("driver");
    }
    @Override
    public Rule[] readRulesFromDB() {
        config = new HikariConfig();
        config.setJdbcUrl( jdbcUrl );
        config.setUsername( username );
        config.setPassword( password );
        ds = new HikariDataSource(config);
        try(Connection connection = ds.getConnection()){
            DSLContext dslcontext = DSL.using(connection, SQLDialect.POSTGRES);
            log.debug("Created a new datasource");
            rules = dslcontext
                    .select()
                    .from("filter_rules")
                    .fetchInto(Rule.class)
                    .toArray(Rule[]::new);
            log.debug("Got rules");
        } catch(SQLException e){
            log.error("An error occurred while reading rules from the database: {}", e.getMessage());
        }


        return rules;
    }
}
