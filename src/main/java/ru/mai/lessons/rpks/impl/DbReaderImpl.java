package ru.mai.lessons.rpks.impl;

import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import lombok.extern.slf4j.Slf4j;
import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import static org.jooq.impl.DSL.field;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

@Slf4j
public class DbReaderImpl implements DbReader, AutoCloseable {
    private final HikariDataSource ds;

    DbReaderImpl(Config config) {
        HikariConfig hikConf = new HikariConfig();
        hikConf.setJdbcUrl(config.getString("jdbcUrl"));
        hikConf.setPassword(config.getString("password"));
        hikConf.setUsername(config.getString("user"));
        hikConf.addDataSourceProperty("cachePrepStmts", "true");
        hikConf.addDataSourceProperty("prepStmtCacheSize", "250");
        hikConf.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(hikConf);
    }

    private Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    @Override
    public Rule[] readRulesFromDB() {
        DSLContext context = null;
        ArrayList<Rule> result = new ArrayList<>();

        try (Connection connection = getConnection()) {
            context = DSL.using(connection, SQLDialect.POSTGRES);

            var rules = context.select(field("field_name"), field("filter_function_name"),
                    field("filter_value"), field("rule_id"), field("filter_id")).from("filter_rules").fetch();

            if (rules.isEmpty()) {
                log.info("Error! Rules not found!");
            } else {
                rules.forEach(dbRule -> {
                    Rule rule = new Rule();

                    rule.setFieldName((String) dbRule.getValue("field_name"));
                    rule.setFilterFunctionName((String) dbRule.getValue("filter_function_name"));
                    rule.setFilterValue((String) dbRule.getValue("filter_value"));
                    rule.setRuleId((Long) dbRule.getValue("rule_id"));
                    rule.setFilterId((Long) dbRule.getValue("filter_id"));

                    result.add(rule);
                });
            }
        } catch (SQLException e) {
            log.info("Error in DB rules!");
            e.printStackTrace();
        }
        return result.toArray(new Rule[0]);
    }

    @Override
    public void close() {
        ds.close();
    }
}
