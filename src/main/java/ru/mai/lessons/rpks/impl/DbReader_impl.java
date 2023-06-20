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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DbReader_impl implements DbReader {
    private static final HikariConfig hikariConfig = new HikariConfig();
    private final HikariDataSource dataSource;

    public DbReader_impl(Config config) {
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));
        dataSource = new HikariDataSource(hikariConfig);
    }

    @Override
    public Rule[] readRulesFromDB() {
        try (var connection = dataSource.getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            var result = context.select().from("public.filter_rules").fetch();
            List<Rule> ruleList = new ArrayList<>(result.size());

            for (var record : result) {
                Rule rule = Rule.builder()
                        .filterId(record.get("filter_id", Long.class))
                        .ruleId(record.get("rule_id", Long.class))
                        .fieldName(record.get("field_name", String.class))
                        .filterFunctionName(record.get("filter_function_name", String.class))
                        .filterValue(record.get("filter_value", String.class))
                        .build();

                ruleList.add(rule);
            }

            return ruleList.toArray(new Rule[0]);
        } catch (SQLException e) {
            log.error("Failed to read from the database: {}", e.toString());
        }

        return new Rule[0];
    }
}

