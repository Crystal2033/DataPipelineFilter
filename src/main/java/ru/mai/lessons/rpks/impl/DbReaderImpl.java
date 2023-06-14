package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Slf4j
@RequiredArgsConstructor
@Setter
public class DbReaderImpl implements DbReader {
    private DataSource dataSource;

    @NonNull
    Config config;

    String filterId = "filter_id";
    String ruleId = "rule_id";
    String fieldName = "field_name";
    String filterFunctionName = "filter_function_name";
    String filterValue = "filter_value";

    private Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    private DataSource getDataSource() {
        if (dataSource == null) {
            createDataSource();
        }
        log.info("Get datasource successfully");
        return dataSource;
    }

    void createDataSource() {
        HikariConfig hikariConfigConfig = createHikariConfig();
        this.dataSource = new HikariDataSource(hikariConfigConfig);
        log.info("Created a new datasource");
    }

    HikariConfig createHikariConfig() {
        String driver = config.getString("db.driver");
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
        hikariConfig.setUsername(config.getString("db.user"));
        hikariConfig.setPassword(config.getString("db.password"));
        hikariConfig.setDriverClassName(driver);
        log.info("Hikari config was done");
        return hikariConfig;
    }


    @Override
    public Rule[] readRulesFromDB() {
        try (Connection connection = getConnection()){
            DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = config.getString("db.table");
            int countRules = dsl.fetchCount(dsl.selectFrom(tableName));
            Rule[] rules = new Rule[countRules];
            ArrayList<Rule> rulesFromDb = new ArrayList<>();

            var selectFromDb = dsl.select(
                    field(filterId),
                    field(ruleId),
                    field(fieldName),
                    field(filterFunctionName),
                    field(filterValue)
            ).from(table(tableName)).fetch();

            selectFromDb.forEach(row -> {
                Long fieldFilterId = (Long) row.getValue(field(filterId));
                Long fieldRuleId = (Long) row.getValue(field(ruleId));
                String fieldFieldName = row.getValue(field(fieldName)).toString();
                String fieldFilterFunctionName = row.getValue(field(filterFunctionName)).toString();
                String fieldFilterValue = row.getValue(field(filterValue)).toString();
                Rule rule = new Rule(fieldFilterId, fieldRuleId, fieldFieldName, fieldFilterFunctionName, fieldFilterValue);
                rulesFromDb.add(rule);
            });

            rulesFromDb.toArray(rules);
            return rules;
        } catch (SQLException e) {
            log.error("Connection was failed!");
            throw new IllegalStateException("DB is not ready");
        } catch (Exception e) {
            e.printStackTrace();
            return new Rule[0];
        }
    }
}
