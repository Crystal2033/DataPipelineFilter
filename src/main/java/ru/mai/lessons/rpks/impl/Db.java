package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;
import java.time.Duration;

import javax.sql.DataSource;
import java.beans.ConstructorProperties;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;

import static org.jooq.impl.DSL.*;

public class Db {
    private static DataSource dataSource;
    Connection conn;
        public Rule[] readRulesFromDB(DSLContext context, int updateIntervalSec) {

            String tableName = "filter_rules";


//            final long interval = updateIntervalSec * 1000;
//            long lastLogged = System.currentTimeMillis();
//            while (processing) {
//                if (System.currentTimeMillis() - lastLogged > interval) {
//                    lastLogged = System.currentTimeMillis();
//                    updateDataBase();
//                }
//                // do rest of processing
//            }
//// Optionally:
//            logToDatabase();



            ArrayList<Rule> array= new ArrayList<>();
//            DSLContext context = DSL.using(conn, SQLDialect.POSTGRES);
//            Query query = context.select(field("filter_id"), field("rule_id"),  field("field_name"),
//                            field("filter_function_name"),
//                            field("filter_value"))
//                    .from(table(tableName));
//
//            String sql = query.getSQL(ParamType.INLINED);
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
//            Rule[] rules = result.toArray();
            result.forEach(res -> {
                try {

                    Long filterId = (Long)res.getValue(field("filter_id"));
                    Long ruleId = (Long)res.getValue(field(name("rule_id")));
                    String field_name = res.getValue(field(name("field_name"))).toString();
                    String filter_function_name = res.getValue(field(name("filter_function_name"))).toString();
                    String filter_value = res.getValue(field(name("filter_value"))).toString();
                    Rule rule = new Rule(filterId, ruleId, field_name, filter_function_name, filter_value);
//                    ArrayList<String> a = new ArrayList<>();
//                    a.add(filterId);
//                    a.add(ruleId);
                    array.add(rule);
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                var result = context.select(
//                        field("filter_id"),
//                        field("rule_id"),
//                        field("field_name"),
//                        field("filter_function_name"),
//                        field("filter_value")
//                )
//                .from(table(tableName)).fetch();
//
//                String stringResult = result.formatCSV();


            });
            array.toArray(ruleArray);
            return ruleArray;
    }
    static Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
    //Get the DataSource. If not available create the new one
    //It is not threadsafe. I didn't wanted to complicate things.
    private static DataSource getDataSource() {
        if (null == dataSource) {
            System.out.println("No DataSource is available. We will create a new one.");
            createDataSource();
        }
        return dataSource;
    }
    //To create a DataSource and assigning it to variable dataSource.
    private static void createDataSource() {
        try {
            Class.forName("org.postgresql.Driver");
            HikariConfig hikariConfig = getHikariConfig();
            System.out.println("Configuration is ready.");
            System.out.println("Creating the HiKariDataSource and assigning it as the global");
            HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
            dataSource = hikariDataSource;
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    //returns HikariConfig containing JDBC connection properties
    //which will be used by HikariDataSource object.
    private static HikariConfig getHikariConfig() {
        System.out.println("Creating the config with HikariConfig");
        HikariConfig hikaConfig = null;
        try {
            hikaConfig = new HikariConfig();
            Class.forName("org.postgresql.Driver");
            //This is same as passing the Connection info to the DriverManager class.
            //your jdbc url. in my case it is mysql.
            hikaConfig.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
            //username
            hikaConfig.setUsername("postgres");
            //password
            hikaConfig.setPassword("postgres");
            //driver class name
            hikaConfig.setDriverClassName("org.postgresql.Driver");
            return hikaConfig;
        } catch (Exception e) {
            e.printStackTrace();
        }

//        // Information about the pool
//        //pool name. This is optional you don't have to do it.
//        hikaConfig.setPoolName("MysqlPool-1");
//        //the maximum connection which can be created by or resides in the pool
//        hikaConfig.setMaximumPoolSize(5);
//        //how much time a user can wait to get a connection from the pool.
//        //if it exceeds the time limit then a SQlException is thrown
//        hikaConfig.setConnectionTimeout(Duration.ofSeconds(30).toMillis());
//        //The maximum time a connection can sit idle in the pool.
//        // If it exceeds the time limit it is removed form the pool.
//        // If you don't want to retire the connections simply put 0.
//        hikaConfig.setIdleTimeout(Duration.ofMinutes(2).toMillis());
        return hikaConfig;
    }



}
