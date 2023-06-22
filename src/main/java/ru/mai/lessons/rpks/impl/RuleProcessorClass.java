package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
@Slf4j
public class RuleProcessorClass implements ru.mai.lessons.rpks.RuleProcessor {

    private ObjectMapper mapper;


    enum FilterFunction {
        CONTAINS,
        NOT_CONTAINS,
        EQUALS,
        NOT_EQUALS;
    }

    public RuleProcessorClass(){
        this.mapper = new ObjectMapper();
    }



    private boolean checkRules(String filterFunctionName, String filterValue, String fieldValue) {

        FilterFunction function = FilterFunction.valueOf(filterFunctionName.toUpperCase());

        switch (function) {
            case CONTAINS:
                log.info("Rule \"contains\" " + fieldValue.contains(filterValue));
                return fieldValue.contains(filterValue);
            case NOT_CONTAINS:
                log.info("Rule \"not_contains\" " + !fieldValue.contains(filterValue));
                return !fieldValue.contains(filterValue);
            case EQUALS:
                log.info("Rule \"equals\" " + fieldValue.equals(filterValue));
                return fieldValue.equals(filterValue);
            case NOT_EQUALS:
                log.info("Rule \"not_equals\" " + !fieldValue.equals(filterValue));
                return !fieldValue.equals(filterValue);
            default:
                return false;
        }
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        message.setFilterState(true);
        try {
            Map<Object, Object> map = mapper.readValue(message.getValue(), Map.class);
            if (rules == null || rules.length == 0) {
                message.setFilterState(false);
            } else {
                for (Rule rule : rules) {
                    if (map.containsKey(rule.getFieldName())) {
                        message.setFilterState(checkRules(rule.getFilterFunctionName(),
                                rule.getFilterValue(),
                                map.get(rule.getFieldName()).toString()));
                        if (!message.isFilterState()) {
                            return message;
                        }
                    } else {
                        message.setFilterState(false);
                        break;
                    }
                }
            }
        } catch (JsonProcessingException e) {
            log.error("Exception while processing JSON", e);
            message.setFilterState(false);
        } catch (Exception e) {
            log.error("Exception occurred", e);
            message.setFilterState(false);
        }
        return message;
    }
}