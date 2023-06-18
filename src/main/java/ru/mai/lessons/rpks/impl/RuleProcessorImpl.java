package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

public class RuleProcessorImpl implements RuleProcessor {
    ObjectMapper mapper = new ObjectMapper();
    @Override
    public Message processing(Message message, Rule[] rules) {

        try {
            JsonNode jsonNode = mapper.readTree(message.getValue());

            for (var rule : rules) {
                JsonNode temp = jsonNode.get(rule.getFieldName());
                if (!temp.isTextual())
                {
                    message.setFilterState(false);
                    return message;
                }

                String value = temp.textValue();
                if (Objects.equals(rule.getFilterFunctionName(), "equals")
                        && Objects.equals(value, rule.getFilterValue())) {
                    message.setFilterState(true);
                }
                else if (Objects.equals(rule.getFilterFunctionName(), "contains")
                        && rule.getFilterValue().contains(value)) {
                    message.setFilterState(true);
                }
                else if (Objects.equals(rule.getFilterFunctionName(), "not_equals")
                        && !Objects.equals(value, rule.getFilterValue())) {
                    message.setFilterState(true);
                }
                else if (Objects.equals(rule.getFilterFunctionName(), "not_contains")
                        && !rule.getFilterValue().contains(value)) {
                    message.setFilterState(true);
                }
                else{
                    message.setFilterState(false);
                    return  message;
                }
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return message;
    }
}
