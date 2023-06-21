package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Operation;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class KafkaRuleProcessor implements RuleProcessor {

    ObjectMapper mapper = new ObjectMapper();
    @Override
    public Message processing(Message message, Rule[] rules) {
        Map<String, String> map;
        try {
            map = mapper.readValue(message.getValue(), new TypeReference<Map<String, String>>() {});

        message.setFilterState(false);
        for (Rule rule : rules) {
            if (!map.containsKey(rule.getFieldName()) || map.get(rule.getFieldName()) == null) {
                log.debug("Message {} have uncorrected data in field {}", message.getValue(), rule.getFieldName());
                message.setFilterState(false);
                return message;
            }
            String function = rule.getFilterFunctionName();

            if (Objects.equals(function, Operation.EQUALS.label) &&
                    Objects.equals(rule.getFilterValue(), map.get(rule.getFieldName()))) {
                        message.setFilterState(true);
                    }
            else if (Objects.equals(function, Operation.NOT_EQUALS.label) &&
                    !Objects.equals(rule.getFilterValue(), map.get(rule.getFieldName()))) {
                        message.setFilterState(true);
                    }
            else if (Objects.equals(function, Operation.CONTAINS.label) &&
                    map.get(rule.getFieldName()).contains(rule.getFilterValue())) {
                        message.setFilterState(true);
                    }
            else if (Objects.equals(function, Operation.NOT_CONTAINS.label) &&
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
            log.error("Message {} have uncorrected data", message.getValue());
            message.setFilterState(false);
            return message;
        }
    }

}
