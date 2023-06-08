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
    private enum FilterFunction {
        EQUALS, CONTAINS, NOT_EQUALS, NOT_CONTAINS
    }
    private static final Map<FilterFunction, BiPredicate<String, String> > checkMap = Map.of(
        FilterFunction.EQUALS, String::equals
        , FilterFunction.CONTAINS, String::contains
        , FilterFunction.NOT_EQUALS, (s1, s2) -> !s1.equals(s2)
        , FilterFunction.NOT_CONTAINS, (s1, s2) -> !s1.contains(s2)
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
            log.debug("message: {}", value);
            message.setFilterState(true);
            for (var rule: rules) {
                String fieldValue = jsonNode.path(rule.getFieldName()).asText("");
                try {
                    if (fieldValue.isBlank() || !checkMap.get(FilterFunction.valueOf(rule.getFilterFunctionName().toUpperCase()))
                            .test(fieldValue, rule.getFilterValue())) {
                        message.setFilterState(false);
                        break;
                    }
                }
                catch (IllegalArgumentException e) {
                    message.setFilterState(false);
                    log.error("Illegal filter function: {}", rule.getFilterFunctionName());
                    break;
                }
                log.debug("fieldValue: {}", fieldValue);
                log.debug("FilterFunctionName: {}", rule.getFilterFunctionName());
                log.debug("FilterValue: {}", rule.getFilterValue());
            }
        }
        catch (JsonProcessingException  e) {
            message.setFilterState(false);
        }
        return message;
    }
}
