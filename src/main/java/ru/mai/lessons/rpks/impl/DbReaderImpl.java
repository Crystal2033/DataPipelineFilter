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

@Slf4j
public class DbReaderImpl implements DbReader {
    private static final HikariConfig hikariConfig = new HikariConfig();
    private final HikariDataSource ds;
    public DbReaderImpl(Config config) {
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));
        ds = new HikariDataSource(hikariConfig);
    }

    private Rule.FunctionName setMsgType(String type){
        switch (type) {
            case "equals" -> {
                return Rule.FunctionName.EQUALS;
            }
            case "contains" -> {
                return Rule.FunctionName.CONTAINS;
            }
            case "not_equals" -> {
                return Rule.FunctionName.NOT_EQUALS;
            }
            case "not_contains" -> {
                return Rule.FunctionName.NOT_CONTAINS;
            }
            default -> {
                return Rule.FunctionName.OTHER;
            }
        }
    }

    @Override
    public Rule[] readRulesFromDB() {
        try {
            var con = ds.getConnection();
            DSLContext context = DSL.using(con, SQLDialect.POSTGRES);
            return context.select().from("public.filter_rules").
                    fetch().
                    stream().
                    map(re ->Rule.builder()
                            .filterId((Long) re.getValue("filter_id"))
                            .ruleId((Long) re.getValue("rule_id"))
                            .fieldName((String) re.getValue("field_name"))
                            .filterFunctionName((String) re.getValue("filter_function_name"))
                            .filterValue((String) re.getValue("filter_value"))
                            .filterFunctionNameEnum(setMsgType((String) re.getValue("filter_function_name")))
                            .build()
                    ).toList().toArray(Rule[]::new);
            
        } catch (SQLException e) {
            log.error("can't read from db error: {}", e.toString());
        }
        return new Rule[0];
    }
}
