package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jooq.*;
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
    private static DataSource dataSource;
    static String username ="";
    static String password ="";
    static String jdbcUrl="";
    static String driverClassName = "";

    public Rule[] readRulesFromDB(DSLContext context) {

        String tableName = "filter_rules";
        ArrayList<Rule> array= new ArrayList<>();
        Result<Record5<Object, Object, Object, Object, Object>> result = context.select(
                        field("filter_id"),
                        field("rule_id"),
                        field("field_name"),
                        field("filter_function_name"),
                        field("filter_value")
                )
                .from(table(tableName)).fetch();
        int numberOfRows = context.fetchCount(context.selectFrom(tableName));
        Rule[] ruleArray = new Rule[numberOfRows];

        result.forEach(res -> {
            try {

                Long filterId = (Long)res.getValue(field("filter_id"));
                Long ruleId = (Long)res.getValue(field(name("rule_id")));
                String fieldName = res.getValue(field(name("field_name"))).toString();
                String filterFunctionName = res.getValue(field(name("filter_function_name"))).toString();
                String filterValue = res.getValue(field(name("filter_value"))).toString();
                Rule rule = new Rule(filterId, ruleId, fieldName, filterFunctionName, filterValue);
                array.add(rule);
            } catch (Exception e) {
                e.printStackTrace();
            }
//            try {
//                closeConnection();
//                log.info("CONNECTION IS CLOSED");
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }

        });
        array.toArray(ruleArray);
        return ruleArray;
    }
    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
    static void closeConnection() throws SQLException {
        getDataSource().getConnection().close();
    }

    private static DataSource getDataSource() {
        if (null == dataSource) {
            log.info("No DataSource is available. We will create a new one.");
            createDataSource();
        }
        return dataSource;
    }

    private static void createDataSource() {
        try {
            Class.forName(driverClassName);
            HikariConfig hikariConfig = getHikariConfig();
            log.info("Configuration is ready.");
            log.info("Creating the HiKariDataSource and assigning it as the global");
            HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
            dataSource = hikariDataSource;
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private static HikariConfig getHikariConfig() {
        log.info("Creating the config with HikariConfig");
        HikariConfig hikaConfig = null;
        try {
            hikaConfig = new HikariConfig();
            Class.forName(driverClassName);
            hikaConfig.setJdbcUrl(jdbcUrl);
            //username
            hikaConfig.setUsername(username);
            //password
            hikaConfig.setPassword(password);
            //driver class name
            hikaConfig.setDriverClassName(driverClassName);
            return hikaConfig;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return hikaConfig;
    }


    static void setJdbcUrl(@NotNull String url) {
        jdbcUrl =  url;
    }

    static void setUsername(@NotNull String user) {
        username = user;
    }
    static void setPassword(@NotNull String pass) {
        password = pass;
    }
    static void setDriverClassName(@NotNull String driver){
        driverClassName = driver;
    }
}