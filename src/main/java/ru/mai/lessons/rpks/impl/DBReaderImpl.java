package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record5;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.jooq.impl.DSL.field;

@Slf4j
public class DBReaderImpl implements DbReader, AutoCloseable {

    private final HikariConfig configHikari = new HikariConfig();
    private HikariDataSource ds;
    private Connection connection;

    DBReaderImpl(Config config) {
        configHikari.setJdbcUrl(config.getString("jdbcUrl"));
        configHikari.setPassword(config.getString("password"));
        configHikari.setUsername(config.getString("user"));
        configHikari.addDataSourceProperty("cachePrepStmts", "true");
        configHikari.addDataSourceProperty("prepStmtCacheSize", "250");
        configHikari.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(configHikari);
        setConnection();
    }

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    private void setConnection() {
        try {
            connection = ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Rule[] readRulesFromDB() {
        DSLContext context = null;
        ArrayList<Rule> rulesRes = new ArrayList<>();
        try {
            if (connection.isClosed()) {
                setConnection();
            }
            context = DSL.using(connection, SQLDialect.POSTGRES);

            log.info("debug " + FIELDDBNAME.FIELDNAME.getNameField());

            Result<Record5<Object, Object, Object, Object, Object>> rules = context.select(field(FIELDDBNAME.FIELDNAME.getNameField()), field(FIELDDBNAME.FUNCTIONNAME.getNameField()), field(FIELDDBNAME.FILTERVALUE.getNameField()), field(FIELDDBNAME.RULEID.getNameField()), field(FIELDDBNAME.FILTERID.getNameField())).from("filter_rules").fetch();

            if (rules.isEmpty()) {
                log.info("rules is empty");
            }

            rules.forEach(ruleDb -> {
                log.info("foreach");
                Rule rule = new Rule();

                rule.setFilterFunctionName((String) ruleDb.getValue(FIELDDBNAME.FUNCTIONNAME.getNameField()));
                rule.setFilterValue((String) ruleDb.getValue(FIELDDBNAME.FILTERVALUE.getNameField()));
                rule.setFieldName((String) ruleDb.getValue(FIELDDBNAME.FIELDNAME.getNameField()));
                rule.setRuleId((Long) ruleDb.getValue(FIELDDBNAME.RULEID.getNameField()));
                rule.setFilterId((Long) ruleDb.getValue(FIELDDBNAME.FILTERID.getNameField()));

                rulesRes.add(rule);
            });
            log.info(String.valueOf(rulesRes));
        } catch (SQLException e) {
            e.printStackTrace();
            log.info("sql exception ");
        }

        return rulesRes.toArray(new Rule[0]);
    }

    @Override
    public void close() throws SQLException {
        ds.close();
    }

    private enum FIELDDBNAME {
        FIELDNAME("field_name"), FILTERVALUE("filter_value"), RULEID("rule_id"), FILTERID("filter_id"), FUNCTIONNAME("filter_function_name");

        private final String nameField;

        FIELDDBNAME(String nameField) {
            this.nameField = nameField;
        }

        public String getNameField() {
            return nameField;
        }
    }

}
