package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.jooq.model.Tables;
import ru.mai.lessons.rpks.jooq.model.tables.FilterRules;
import ru.mai.lessons.rpks.jooq.model.tables.records.FilterRulesRecord;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


public final class DbReaderImpl implements DbReader {
    private HikariDataSource hikariDataSource;

    public DbReaderImpl(Config dbConfig)  {
        String url = dbConfig.getString("jdbcUrl");
        String user = dbConfig.getString("user");
        String password = dbConfig.getString("password");
        String driver = dbConfig.getString("driver");

        HikariConfig dataBaseConfig = new HikariConfig();
        dataBaseConfig.setJdbcUrl(url);
        dataBaseConfig.setUsername(user);
        dataBaseConfig.setPassword(password);
        dataBaseConfig.setDriverClassName(driver);

        this.hikariDataSource = new HikariDataSource(dataBaseConfig);
    }

    @Override
    public Rule[] readRulesFromDB() {
        try {
            return tryToReadRulesFromDB();
        } catch (SQLException e) {
            throw new IllegalStateException("Can't get rules from DB!");
        }
    }
    private Rule[] tryToReadRulesFromDB() throws SQLException {
        Connection connection = this.hikariDataSource.getConnection();
        DSLContext dslContext = DSL.using(connection, SQLDialect.POSTGRES);

        return dslContext
                .select()
                .from(Tables.FILTER_RULES)
                .fetchInto(Rule.class)
                .toArray(Rule[]::new);
    }
}
