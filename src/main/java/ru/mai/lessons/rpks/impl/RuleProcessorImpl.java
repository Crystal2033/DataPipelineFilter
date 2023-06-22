package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import com.fasterxml.jackson.databind.ObjectMapper;


@Slf4j
@Builder
public class RuleProcessorImpl implements RuleProcessor {
    public enum FilterFunction{
        EQUALS, CONTAINS, NOT_EQUALS, NOT_CONTAINS
    }
    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules.length == 0) {
            message.setFilterState(false);
            return message;
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(message.getValue());
            for (Rule rule :rules) {
                boolean res = false;
                String messageValue = node.path(rule.getFieldName()).asText();
                log.debug("in: " + messageValue + " ruleval: " + rule.getFilterValue() + " rul: " + rule.getFilterFunctionName() + "\n");
                if (!messageValue.isBlank()) {
                    FilterFunction rules1 = FilterFunction.valueOf(rule.getFilterFunctionName().toUpperCase());
                    if (rules1.equals(FilterFunction.EQUALS)){
                        res = messageValue.equals(rule.getFilterValue());
                    }
                    if (rules1.equals(FilterFunction.CONTAINS)){
                        res = messageValue.contains(rule.getFilterValue());
                    }
                    if (rules1.equals(FilterFunction.NOT_EQUALS)){
                        res = (!messageValue.equals(rule.getFilterValue()));
                    }
                    if (rules1.equals(FilterFunction.NOT_CONTAINS)){
                        res = !messageValue.contains(rule.getFilterValue());
                    }
                }
                message.setFilterState(res);
                if (!res) {
                    break;
                }
            }
        } catch (JsonProcessingException e) {
            message.setFilterState(false);
            log.error("json processing exception");
        }
        return message;
    }

}
