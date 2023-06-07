package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
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
    String fieldNameFilterId = "filter_id";
    String ruleIdStr = "rule_id";
    String fieldNameStr = "field_name";
    String filterFunctionNameStr = "filter_function_name";
    String filterValueStr = "filter_value";

public Rule[] readRulesFromDB() {
    try (Connection connection = getConnection()) {
        DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
        String tableName = config.getConfig("db").getString("table");

        int numberOfRows = context.fetchCount(context.selectFrom(tableName));
        Rule[] ruleArray = new Rule[numberOfRows];
//           there was a @NotNull SelectJoinStep<Record5<Object, Object, Object, Object, Object>> before
            ArrayList<Rule> array = new ArrayList<>();
            var result = context.select(
                            field(fieldNameFilterId),
                            field(ruleIdStr),
                            field(fieldNameStr),
                            field(filterFunctionNameStr),
                            field(filterValueStr)
                    )
                    .from(table(tableName)).fetch();

//            there was var result = result2.fetch() before

            result.forEach(res -> {
                try {

                    Long filterId = (Long) res.getValue(field(fieldNameFilterId));
                    Long ruleId = (Long) res.getValue(field(name(ruleIdStr)));
                    String fieldName = res.getValue(field(name(fieldNameStr))).toString();
                    String filterFunctionName = res.getValue(field(name(filterFunctionNameStr))).toString();
                    String filterValue = res.getValue(field(name(filterValueStr))).toString();
                    Rule rule = new Rule(filterId, ruleId, fieldName, filterFunctionName, filterValue);
                    array.add(rule);
                } catch (Exception e) {
                    log.error("caught rule exception");
                }
            });
            array.toArray(ruleArray);
            return ruleArray;
        }
     catch (SQLException e) {
        log.error("DB rules error!");
        throw new IllegalStateException("DB rules error");
    }
    catch (Exception e) {
        log.error("CAUGHT FETCH EX");
        return new Rule[0];
    }

}




    Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
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
            log.error("caught datasource exception");
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
            log.error("caught hikaconfig exception");
        }

        return hikaConfig;
    }

}