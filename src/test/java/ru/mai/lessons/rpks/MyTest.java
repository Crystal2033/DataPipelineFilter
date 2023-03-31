package ru.mai.lessons.rpks;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import ru.mai.lessons.rpks.impl.ConfigReaderImpl;

import java.sql.Connection;
import java.sql.SQLException;

public class MyTest {
    @Test
    void myTestTryToParseConfigAndRunDB() {
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
            assert (true);
        } catch (SQLException e) {
            e.printStackTrace();
            assert (false);
        }
    }





}
