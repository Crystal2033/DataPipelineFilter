package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;

import com.typesafe.config.Config;

import java.sql.SQLException;
import java.util.ArrayList;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class MyDbReader implements DbReader {
    private HikariDataSource ds;

    public MyDbReader(Config config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setUsername(config.getString("user"));
        hikariConfig.setPassword(config.getString("password"));
        hikariConfig.setJdbcUrl(config.getString("jdbcUrl"));
        ds = new HikariDataSource(hikariConfig);

    }

    private Rule.FunctionName getFunctionName(String nameString) {
        switch (nameString) {
            case "equals" -> {
                return Rule.FunctionName.EQUALS;
            }
            case "not_equals" -> {
                return Rule.FunctionName.NOT_EQUALS;
            }
            case "contains" -> {
                return Rule.FunctionName.CONTAINS;
            }
            case "not_contains" -> {
                return Rule.FunctionName.NOT_CONTAINS;
            }
            default -> {
                return null;
            }
        }
    }

    @Override
    public Rule[] readRulesFromDB() {
        ArrayList<Rule> result = new ArrayList<>();
        try {
            Connection connection = ds.getConnection();
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            Result<Record> rules = context.select().from("public.filter_rules").fetch();
            rules.forEach(e -> {
                Long filterId = (Long) e.getValue("filter_id");
                Long ruleId = (Long) e.getValue("rule_id");
                String fieldName = (String) e.getValue("field_name");
                Rule.FunctionName filterFunction = getFunctionName((String) e.getValue("filter_function_name"));
                String filterValue = (String) e.getValue("filter_value");
                Rule rule = new Rule(filterId, ruleId, fieldName, filterFunction, filterValue);
                result.add(rule);
            });

            connection.close();
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

        return result.toArray(new Rule[0]);
    }
}
