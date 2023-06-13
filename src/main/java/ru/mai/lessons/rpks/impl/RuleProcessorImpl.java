package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Objects;

public class RuleProcessorImpl implements RuleProcessor {
    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);

        try {
            Map<String, Object> msg = mapper.readValue(message.getValue(), Map.class);
            if (rules.length != 0) {
                for (Rule rule: rules) {
                    if (msg.containsKey(rule.getFieldName())) {
                        checkRules(msg, rule, message);
                    }
                    else {
                        message.setFilterState(false);
                        break;
                    }
                }
            }
            else {
                message.setFilterState(false);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            message.setFilterState(false);
        } catch (Exception e) {
            e.printStackTrace();
            message.setFilterState(false);
        }
        return message;
    }

    void checkRules(Map<String, Object> msg, Rule rule, Message message) {
        if (Objects.equals(rule.getFilterFunctionName(), "equals") && !Objects.equals(msg.get(rule.getFieldName()).toString(), rule.getFilterValue())) {
            message.setFilterState(false);
            return;
        }
        if (Objects.equals(rule.getFilterFunctionName(), "contains") && !msg.get(rule.getFieldName()).toString().contains(rule.getFilterValue())) {
            message.setFilterState(false);
            return;
        }
        if (Objects.equals(rule.getFilterFunctionName(), "not_equals") && Objects.equals(msg.get(rule.getFieldName()).toString(), rule.getFilterValue())) {
            message.setFilterState(false);
            return;
        }
        if (Objects.equals(rule.getFilterFunctionName(), "not_contains") && msg.get(rule.getFieldName()).toString().contains(rule.getFilterValue())) {
            message.setFilterState(false);
        }
    }
}
