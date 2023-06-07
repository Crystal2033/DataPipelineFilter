package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Objects;
@Slf4j

public class RuleProcessorImpl implements RuleProcessor {
    boolean isExit = false;

    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, false);
        message.setFilterState(true);

// To put all of the JSON in a Map<String, Object>
        try {
            Map<String, Object> map = mapper.readValue(message.getValue(), Map.class);
            if (rules.length == 0) {
                message.setFilterState(false);
            }
            for (Rule rule : rules) {
                //            ПРОВЕРКА ПРАВИЛ
                log.debug("RULES LENGTH {}", rules.length);
                log.debug("CHECKING FIELD {}", rule.getFieldName());
                if (map.containsKey(rule.getFieldName())) {
                    checkRule(rule, map, message);
                } else {
                    message.setFilterState(false);
                    break;
                }

            }


        } catch (JsonProcessingException e) {
            log.error("exception caught");
            message.setFilterState(false);
        } catch (Exception e) {
            log.error("caught null exception");
            message.setFilterState(false);
        }
        return message;
    }

    private Message checkRule(Rule rule, Map<String, Object> map, Message message) {
        if (Objects.equals(rule.getFilterFunctionName(), "equals") && (!Objects.equals(map.get(rule.getFieldName()).toString(), rule.getFilterValue()))) {
            message.setFilterState(false);
            log.debug("set to false - equals {}", map.get(rule.getFieldName()));
            return message;

        }
        if (Objects.equals(rule.getFilterFunctionName(), "not_equals") && (Objects.equals(map.get(rule.getFieldName()).toString(), rule.getFilterValue()))) {
            message.setFilterState(false);
            log.debug("set to false - not equals");
            return message;

        }
        if (Objects.equals(rule.getFilterFunctionName(), "contains") && (!map.get(rule.getFieldName()).toString().contains(rule.getFilterValue()))) {
            message.setFilterState(false);
            log.debug("set to false - contains");
            return message;

        }
        if (Objects.equals(rule.getFilterFunctionName(), "not_contains") && (map.get(rule.getFieldName()).toString().contains(rule.getFilterValue()))) {
            message.setFilterState(false);
            log.debug("set to false - not contains");
            return message;
        }
        return message;
    }
}

