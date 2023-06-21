package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class KafkaRuleProcessor implements RuleProcessor {

    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map;
        try {
            map = mapper.readValue(message.getValue(), new TypeReference<Map<String, String>>() {});

        message.setFilterState(false);
        for (Rule rule : rules) {
            if (!map.containsKey(rule.getFieldName()) || map.get(rule.getFieldName()) == null) {
                log.info("Message {} have uncorrected data in field {}", message.getValue(), rule.getFieldName());
                message.setFilterState(false);
                return message;
            }
            String function = rule.getFilterFunctionName();

            if (Objects.equals(function, "equals") &&
                    Objects.equals(rule.getFilterValue(), map.get(rule.getFieldName()))) {
                        message.setFilterState(true);
                    }
            else if (Objects.equals(function, "not_equals") &&
                    !Objects.equals(rule.getFilterValue(), map.get(rule.getFieldName()))) {
                        message.setFilterState(true);
                    }
            else if (Objects.equals(function, "contains") &&
                    map.get(rule.getFieldName()).contains(rule.getFilterValue())) {
                        message.setFilterState(true);
                    }
            else if (Objects.equals(function, "not_contains") &&
                    !map.get(rule.getFieldName()).contains(rule.getFilterValue())) {
                        message.setFilterState(true);
                    }
            else {
                message.setFilterState(false);
                return message;
            }
        }

        return message;

        } catch (JsonProcessingException e) {
            log.info("Message {} have uncorrected data", message.getValue());
            message.setFilterState(false);
            return message;
        }
    }

}
