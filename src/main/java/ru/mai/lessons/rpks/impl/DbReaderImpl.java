package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import java.sql.Connection;
import java.sql.SQLException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.jooq.Tables;

@Slf4j
public final class DbReaderImpl implements DbReader {
    private final HikariDataSource hikari;

    public DbReaderImpl(Config dbConfig) {
        String url = dbConfig.getString("jdbcUrl");
        String user = dbConfig.getString("user");
        String pass = dbConfig.getString("password");
        String driver = dbConfig.getString("driver");

        HikariConfig hDbConfig = new HikariConfig();
        hDbConfig.setJdbcUrl(url);
        hDbConfig.setUsername(user);
        hDbConfig.setPassword(pass);
        hDbConfig.setDriverClassName(driver);
        this.hikari = new HikariDataSource(hDbConfig);
    }

    @Override
    public Rule[] readRulesFromDB() {
        try (Connection connection = this.hikari.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
            return dsl
                    .select()
                    .from(Tables.FILTER_RULES)
                    .fetchInto(Rule.class)
                    .toArray(Rule[]::new);
        } catch (SQLException e) {
            throw new IllegalStateException("DB rules error");
        }
    }
}