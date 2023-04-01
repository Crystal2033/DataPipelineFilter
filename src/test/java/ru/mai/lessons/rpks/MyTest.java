package ru.mai.lessons.rpks;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import ru.mai.lessons.rpks.impl.ConfigReaderImpl;
import ru.mai.lessons.rpks.impl.KafkaWriterImpl;
import ru.mai.lessons.rpks.jooq.model.Tables;
import ru.mai.lessons.rpks.jooq.model.tables.FilterRules;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class MyTest {
    @Test
    void myTestTryToParseConfigAndRunDB() throws SQLException {
        ConfigReader configReader = new ConfigReaderImpl();
        Config config = configReader.loadConfig().getConfig("db");

        String url = config.getString("jdbcUrl");
        String user = config.getString("user");
        String password = config.getString("password");
        String driver = config.getString("driver");

        HikariConfig dataBaseConfig = new HikariConfig();
        dataBaseConfig.setJdbcUrl(url);
        dataBaseConfig.setUsername(user);
        dataBaseConfig.setPassword(password);
        dataBaseConfig.setDriverClassName(driver);

        try (
                HikariDataSource ds = new HikariDataSource(dataBaseConfig);
                Connection connection = ds.getConnection();
        ) {
            DSLContext dslContext = DSL.using(connection, SQLDialect.POSTGRES);

            List<Rule> result = dslContext
                    .select()
                    .from(Tables.FILTER_RULES)
                    .fetchInto(Rule.class);

            for (Rule rule : result) {
                System.out.println(rule.getFieldName());
            }
        }
    }

    @Test
    void sendMsg() {

    }


}
