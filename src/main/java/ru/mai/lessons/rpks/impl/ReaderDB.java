package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.*;
import java.util.ArrayList;

@Slf4j
@RequiredArgsConstructor
public class ReaderDB implements DbReader {
    private final String url;
    private final String user;
    private final String password;
    private final String driver;

    public Rule[] readRulesFromDB()
    {
        final String tableName = "filter_rules";
        ArrayList<Rule> listRules = new ArrayList<>();

        try {
            HikariDataSource dataSource = createConnectionPool();
            DSLContext context = DSL.using(dataSource.getConnection(), SQLDialect.POSTGRES);
            var results = context.select()
                    .from(tableName)
                    .fetch();

            results.forEach(result -> {
                // Переменные для создания правила. Считываем из базы данных
                Long filterId = (Long)result.getValue("filter_id");
                Long ruleId = (Long)result.getValue("rule_id");
                String fieldName = (String)result.getValue("field_name");
                String funcName = (String)result.getValue("filter_function_name");
                String value = (String)result.getValue("filter_value");

                Rule rule = new Rule(filterId, ruleId, fieldName, funcName, value);
                listRules.add(rule);
            });
        } catch (SQLException ex) {
            log.error("Error truncate table", ex);
        }

        return listRules.toArray(Rule[]::new);
    }

    private HikariDataSource createConnectionPool() {
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setDriverClassName(driver);
        return new HikariDataSource(config);
    }
}
