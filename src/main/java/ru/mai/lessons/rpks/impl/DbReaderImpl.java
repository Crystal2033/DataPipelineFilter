package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record5;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.config.DbConfig;
import ru.mai.lessons.rpks.exception.ServerException;
import ru.mai.lessons.rpks.model.FilterFunction;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Slf4j
public class DbReaderImpl implements DbReader {

    private final DataSource dataSource;
    private final long updateIntervalSec;

    private Rule[] ruleArray;

    private Instant lastCheckTime;

    public DbReaderImpl(Config config) {
        this.dataSource = DbConfig.createConnectionPool(config);
        this.updateIntervalSec = config.getLong("application.updateIntervalSec");
    }

    @Override
    public Rule[] readRulesFromDB() {
        if (ruleArray == null ||
                Duration.between(lastCheckTime, Instant.now()).toMillis() > updateIntervalSec) {
            ruleArray = init();
            lastCheckTime = Instant.now();
        }
        return ruleArray;
    }

    private Rule[] init() {
        try (Connection connection = dataSource.getConnection()) {
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            String tableName = "filter_rules";
            var result = context.select(
                            field("filter_id"),
                            field("rule_id"),
                            field("field_name"),
                            field("filter_function_name"),
                            field("filter_value")
                    )
                    .from(table(tableName))
                    .fetch();

            return result.stream().map((Record5<Object, Object, Object, Object, Object> r) -> {
                var rule = new Rule();
                rule.setFilterId((Long) r.component1());
                rule.setRuleId((Long) r.component2());
                rule.setFieldName((String) r.component3());
                rule.setFilterFunctionName(FilterFunction.valueOf((String) r.component4()));
                rule.setFilterValue((String) r.component5());
                return rule;
            }).toArray(Rule[]::new);
        } catch (SQLException e) {
            log.error("Не смогли получить соединение из базы");
            throw new ServerException("Не смогли получить соединение из базы", e);
        }
    }
}
