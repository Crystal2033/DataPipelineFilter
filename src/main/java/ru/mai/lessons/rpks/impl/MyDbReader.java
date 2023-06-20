package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import static org.jooq.impl.DSL.*;

import java.sql.Connection;
import java.util.ArrayList;

@Slf4j
public class MyDbReader implements DbReader {
    private final HikariDataSource dataSource;
    public MyDbReader(String dbJdbcUrl, String dbPassword, String dbUser){
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(dbJdbcUrl);
        hikariConfig.setPassword(dbPassword);
        hikariConfig.setUsername(dbUser);
        dataSource = new HikariDataSource(hikariConfig);
    }
    @Override
    public Rule[] readRulesFromDB() {
        ArrayList<Rule> arrayList = new ArrayList<>();
        try(Connection connection = dataSource.getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            var result = context.select().from(table("filter_rules")).fetch();

            result.forEach(res -> {
                Long fieldFilterId = (Long) res.getValue("filter_id");
                Long fieldRuleId = (Long) res.getValue("rule_id");
                String name = (String) res.getValue("field_name");
                String fieldFilterFunctionName = (String) res.getValue("filter_function_name");
                String fieldFilterValue = (String) res.getValue("filter_value");
                Rule rule = new Rule(fieldFilterId, fieldRuleId, name, fieldFilterFunctionName, fieldFilterValue);
                arrayList.add(rule);
            });
        } catch (Exception e) {
            log.error("Connection dataSource exception");
        }
        return arrayList.toArray(Rule[]::new);
    }
}
