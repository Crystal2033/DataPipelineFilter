package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.jooq.model.Tables;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public final class DbReaderImpl implements DbReader {
    private final HikariDataSource hikari;

    DbReaderImpl(Config dbConfig) {
        HikariConfig hDbConfig = new HikariConfig();
        hDbConfig.setJdbcUrl(dbConfig.getString("jdbcUrl"));
        hDbConfig.setUsername(dbConfig.getString("user"));
        hDbConfig.setPassword(dbConfig.getString("password"));
        hDbConfig.setDriverClassName(dbConfig.getString("driver"));
        hikari = new HikariDataSource(hDbConfig);
    }

    @Override
    public Rule[] readRulesFromDB() {
        try (Connection conn = hikari.getConnection()) {
            DSLContext dsl = DSL.using(conn, SQLDialect.POSTGRES);
            return dsl.select().from(Tables.FILTER_RULES).fetchInto(Rule.class).toArray(Rule[]::new);
        } catch (SQLException ex) {
            log.info("SQL error %s".formatted(ex.getMessage()));
        }
        return new Rule[0];
    }
}
