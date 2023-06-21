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
public final class DbReaderImpl implements DbReader {
    private final HikariDataSource hikariDataSource;

    public DbReaderImpl(Config config) {
        String driver = config.getConfig("db").getString("driver");
        String url = config.getConfig("db").getString("jdbcUrl");
        String user = config.getConfig("db").getString("user");
        String password = config.getConfig("db").getString("password");

        HikariConfig dbConfig = new HikariConfig();
        dbConfig.setJdbcUrl(url);
        dbConfig.setUsername(user);
        dbConfig.setPassword(password);
        dbConfig.setDriverClassName(driver);

        hikariDataSource = new HikariDataSource(dbConfig);
        log.info("Connected to db");
    }

    @Override
    public Rule[] readRulesFromDB(){
        try (Connection connection = hikariDataSource.getConnection()) {
            DSLContext dslContext = DSL.using(connection, SQLDialect.POSTGRES);
            return dslContext
                    .select()
                    .from("filter_rules")
                    .fetchInto(Rule.class)
                    .toArray(Rule[]::new);
        } catch (SQLException e) {
            throw new IllegalStateException("Db error, cant get data!");
        }
    }

}
