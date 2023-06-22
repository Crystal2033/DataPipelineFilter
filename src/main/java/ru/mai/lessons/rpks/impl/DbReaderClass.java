package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.model.Rule;

import static org.jooq.impl.DSL.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class DbReaderClass implements ru.mai.lessons.rpks.DbReader {
    private final HikariDataSource dataSource;
    public DbReaderClass(String dbJdbcUrl, String dbPassword, String dbUser){
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(dbJdbcUrl);
        hikariConfig.setUsername(dbUser);
        hikariConfig.setPassword(dbPassword);
        dataSource = new HikariDataSource(hikariConfig);
    }


    public List<Rule> resultToList(Result<Record> result){
        List<Rule> ruleList = new ArrayList<>();
        for (var rec : result) {
            Rule rule = new Rule(rec.get("filter_id", Long.class),
                    rec.get("rule_id", Long.class),
                    rec.get("field_name", String.class),
                    rec.get("filter_function_name", String.class),
                    rec.get("filter_value", String.class));
            ruleList.add(rule);
        }
        return ruleList;
    }


    @Override
    public Rule[] readRulesFromDB() {
        List<Rule> ruleList = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            var result = context.select().from(table("filter_rules")).fetch();
            ruleList = resultToList(result);
        } catch (Exception e) {
            log.error("Exception while making connection", e);
        }
        return ruleList.toArray(new Rule[0]);
    }
}