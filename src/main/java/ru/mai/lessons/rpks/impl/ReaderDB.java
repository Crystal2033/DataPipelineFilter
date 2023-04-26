package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.ConfigReader;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.*;
import java.util.ArrayList;

@Slf4j
@RequiredArgsConstructor
public class ReaderDB implements DbReader {
    private final String url;
    private final String user;
    private final String password;
    public Rule[] readRulesFromDB()
    {
        ArrayList<Rule> listRules = new ArrayList<>();

        String sql = "SELECT * FROM filter_rules";
        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)){

            while(resultSet.next()){
                // Переменные для создания правила. Считываем из базы данных
                Long filterId = resultSet.getLong("filter_id");
                Long ruleId = resultSet.getLong("rule_id");
                String fieldName = resultSet.getString("field_name");
                String funcName = resultSet.getString("filter_function_name");
                String value = resultSet.getString("filter_value");

                Rule rule = new Rule(filterId, ruleId, fieldName, funcName, value);
                listRules.add(rule);
            }
        } catch (SQLException e) {
            log.warn("SQLException!", e);
        }

        return listRules.toArray(Rule[]::new);
    }
}
