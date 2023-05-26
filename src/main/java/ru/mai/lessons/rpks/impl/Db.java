package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jooq.*;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;


import static org.jooq.impl.DSL.*;

@Slf4j
@RequiredArgsConstructor
@Setter
public class Db implements DbReader {
    private DataSource dataSource;
    @NonNull
    Config config;

public Rule[] readRulesFromDB() {
    try (Connection connection = getConnection()) {
        DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
        String tableName = "filter_rules";
        int numberOfRows = context.fetchCount(context.selectFrom(tableName));
        Rule[] ruleArray = new Rule[numberOfRows];
            Result<Record5<Object, Object, Object, Object, Object>> result = null;
            @NotNull SelectJoinStep<Record5<Object, Object, Object, Object, Object>> result2;
            ArrayList<Rule> array = new ArrayList<>();
            String fieldNameFilterId = "filter_id";
//            try {
            result2 = context.select(
                            field(fieldNameFilterId),
                            field("rule_id"),
                            field("field_name"),
                            field("filter_function_name"),
                            field("filter_value")
                    )
                    .from(table(tableName));
//            @NotNull SelectSelectStep<Record1<Object>> filterId;
//            filterId = context.select(field("filter_id"));



            log.info("RESULT2 {}", result2);

            result = result2.fetch();

            log.info("RESULT {}", result.getValues(fieldNameFilterId).isEmpty());

//            } catch (Exception e) {
//                log.info("CAUGHT FETCH EX");
//                return new Rule[0];
//            }


            result.forEach(res -> {
                try {

                    Long filterId = (Long) res.getValue(field(fieldNameFilterId));
                    Long ruleId = (Long) res.getValue(field(name("rule_id")));
                    String fieldName = res.getValue(field(name("field_name"))).toString();
                    String filterFunctionName = res.getValue(field(name("filter_function_name"))).toString();
                    String filterValue = res.getValue(field(name("filter_value"))).toString();
                    Rule rule = new Rule(filterId, ruleId, fieldName, filterFunctionName, filterValue);
                    array.add(rule);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            array.toArray(ruleArray);
            return ruleArray;
        }
     catch (SQLException e) {
        log.info("DB rules error!");
        throw new IllegalStateException("DB rules error");
    }
    catch (Exception e) {
        log.info("CAUGHT FETCH EX");
        return new Rule[0];
    }

}




    Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
    void closeConnection() throws SQLException {
        getDataSource().getConnection().close();
    }

    private DataSource getDataSource() {
        if (null == dataSource) {
            log.info("No DataSource is available. We will create a new one.");
            createDataSource();
        }
        return dataSource;
    }

    private void createDataSource() {
        try {
            String driver = config.getString("db.driver");
            Class.forName(driver);
            HikariConfig hikariConfig = getHikariConfig();
            log.info("Configuration is ready.");
            log.info("Creating the HiKariDataSource and assigning it as the global");
            dataSource = new HikariDataSource(hikariConfig);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private HikariConfig getHikariConfig() {
        log.info("Creating the config with HikariConfig");
        String driver = config.getString("db.driver");
        HikariConfig hikaConfig = null;
        try {
            hikaConfig = new HikariConfig();
            Class.forName(driver);
            hikaConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
            //username
            hikaConfig.setUsername(config.getString("db.user"));
            //password
            hikaConfig.setPassword(config.getString("db.password"));
            //driver class name
            hikaConfig.setDriverClassName(driver);
            return hikaConfig;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return hikaConfig;
    }

}