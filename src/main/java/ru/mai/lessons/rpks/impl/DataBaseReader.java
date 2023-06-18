package ru.mai.lessons.rpks.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;

@Getter
@Setter
@Slf4j
public class DataBaseReader implements DbReader {

    // DB params
    private String url;
    private String pass;
    private String user;
    private String driver;

    // Hikari
    private final HikariConfig config = new HikariConfig();
    private static HikariDataSource hikariDataSource = new HikariDataSource();

    // JOOQ to make requests
    private DSLContext dslContext;

    @Override
    public Rule[] readRulesFromDB() {
        return dslContext.select().from("public.filter_rules").fetchInto(Rule.class).toArray(Rule[]::new);
    }

    public void setHikariParams() {
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(pass);
        //config.setDriverClassName(driver);
    }
    public boolean connectToDataBase() throws SQLException {
        log.info("Init HikariDataSource");
        hikariDataSource = new HikariDataSource(config);
        log.info("Init DSL");
        dslContext = DSL.using(hikariDataSource, SQLDialect.POSTGRES);

        return isConnected();
    }

    public static Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    public boolean isConnected() throws SQLException {
        return getConnection().isValid(0);
    }

    public void close(){
        log.info("Close connection");
        if (hikariDataSource != null)
            hikariDataSource.close();
    }
}
