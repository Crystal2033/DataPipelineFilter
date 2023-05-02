package ru.mai.lessons.rpks.impl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.function.BiPredicate;
@Slf4j
public class MessageRuleProcessor implements RuleProcessor {
    private final ObjectMapper parser = new ObjectMapper();
    private static final Map<String, BiPredicate<String, String> > checkMap = Map.of(
        "equals", String::equals
        , "contains", String::contains
        , "not_equals", (s1, s2) -> !s1.equals(s2)
        , "not_contains", (s1, s2) -> !s1.contains(s2)
    );
    @Override
    public Message processing(Message message, Rule[] rules) {
        String value = message.getValue();
        if (rules.length == 0) {
            message.setFilterState(false);
            return message;
        }

        try {
            JsonNode jsonNode = parser.readTree(value);
            log.info("message: {}", value);
            message.setFilterState(true);
            for (var rule: rules) {
                String fieldValue = jsonNode.path(rule.getFieldName()).asText("");
                if (fieldValue.isBlank() || !checkMap.get(rule.getFilterFunctionName())
                        .test(fieldValue, rule.getFilterValue())) {
                    message.setFilterState(false);
                    break;
                }
                log.info("fieldValue: {}", fieldValue);
                log.info("FilterFunctionName: {}", rule.getFilterFunctionName());
                log.info("FilterValue: {}", rule.getFilterValue());
            }
        }
        catch (JsonProcessingException  e) {
            message.setFilterState(false);
        }
        return message;
    }
}
