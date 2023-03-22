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
public class    MyDbReader implements DbReader {
    private String jdbcUrl;
    private String username;
    private String password;

    public MyDbReader(Config config){
        username = "user";
        password = "password";
        jdbcUrl = config.getString("jdbcUrl");
    }
    @Override
    public Rule[] readRulesFromDB() {
        ArrayList<Rule> result = new ArrayList<>();
        HikariConfig config = new HikariConfig();
        config.setUsername(username);
        config.setPassword(password);
        config.setJdbcUrl(jdbcUrl);
        try (HikariDataSource ds = new HikariDataSource(config)){

            config.setJdbcUrl(jdbcUrl);
            config.setUsername(username);
            config.setPassword(password);
            Connection connection;
            connection = ds.getConnection();
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);

            Result<Record> rules = context.select().from("public.filter_rules").fetch();
            rules.forEach(e -> {
                //filter_id | rule_id | field_name | filter_function_name | filter_value
                Long filterId = (Long) e.getValue("filter_id");
                Long ruleId = (Long) e.getValue("rule_id");
                String fieldName = (String) e.getValue("field_name");
                String filterFunction = (String) e.getValue("filter_function_name");
                String filterValue = (String) e.getValue("filter_value");
                Rule rule = new Rule(filterId, ruleId, fieldName, filterFunction, filterValue);
                result.add(rule);
            });
            connection.close();
        } catch (SQLException e) {
            log.error(e.getMessage());
        }catch (Exception e){
            log.error("some");
        }
        return result.toArray(new Rule[0]);
    }
}
