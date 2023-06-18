package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DbReaderImpl implements DbReader {
    private static final HikariConfig hikariConfig = new HikariConfig();
    private static HikariDataSource ds;
    public DbReaderImpl(Config config) {
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(config.getString("db.driver"));
        ds = new HikariDataSource(hikariConfig);
    }

    @Override
    public Rule[] readRulesFromDB() {
        List<Rule> rules = new ArrayList<>();
        try {
            var con = ds.getConnection();
            DSLContext context = DSL.using(con, SQLDialect.POSTGRES);
            Result<Record> res = context.select().from("public.filter_rules").fetch();

            res.forEach(re ->{
                Long filterId = (Long) re.getValue("filter_id");
                Long ruleId = (Long) re.getValue("rule_id");
                String fieldName = (String) re.getValue("field_name");
                String filterFunctionName = (String) re.getValue("filter_function_name");
                String filterValue = (String) re.getValue("filter_value");
                Rule rule = new Rule(filterId, ruleId, fieldName, filterFunctionName, filterValue);
                rules.add(rule);
            });
            
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return rules.toArray(Rule[]::new);
    }
}
