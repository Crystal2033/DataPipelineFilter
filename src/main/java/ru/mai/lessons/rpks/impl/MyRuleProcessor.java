package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
@Slf4j
public class MyRuleProcessor implements RuleProcessor {
    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        message.setFilterState(true);
        try {
            Map<Object, Object> map = mapper.readValue(message.getValue(), Map.class);

            if (rules == null || rules.length == 0) {
                message.setFilterState(false);
            }
            else {
                for (Rule rule : rules) {
                    if (map.containsKey(rule.getFieldName())) {
                        String filterFunctionName = rule.getFilterFunctionName();
                        String filterValue = rule.getFilterValue();
                        String fieldValue = map.get(rule.getFieldName()).toString();
                        message.setFilterState(checkRules(filterFunctionName, filterValue, fieldValue));
                        if(!message.isFilterState())
                            return message;
                    }
                    else {
                        message.setFilterState(false);
                        break;
                    }
                }
            }
        } catch (JsonProcessingException e) {
            log.error("exception json");
            message.setFilterState(false);
        } catch (Exception e) {
            log.error("exception null");
            message.setFilterState(false);
        }
        return message;
    }

    private boolean checkRules(String filterFunctionName, String filterValue, String fieldValue) {
        switch (filterFunctionName.toLowerCase()) {
            case "equals" -> {
                log.info("equals " + fieldValue.equals(filterValue));
                return fieldValue.equals(filterValue);
            }
            case "not_equals" -> {
                log.info("not_equals " + !fieldValue.equals(filterValue));
                return !fieldValue.equals(filterValue);
            }
            case "contains" -> {
                log.info("contains " + fieldValue.contains(filterValue));
                return fieldValue.contains(filterValue);
            }
            case "not_contains" -> {
                log.info("not_contains " + !fieldValue.contains(filterValue));
                return !fieldValue.contains(filterValue);
            }
            default -> {
                return false;
            }
        }
    }
}
