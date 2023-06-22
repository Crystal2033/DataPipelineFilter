package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Objects;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    ObjectMapper mapper = new ObjectMapper();

    boolean compare(String value, Rule rule){
        switch (rule.getFilterFunctionNameEnum()){
            case EQUALS -> {
                return Objects.equals(value, rule.getFilterValue());
            }
            case CONTAINS -> {
                return value.contains(rule.getFilterValue());
            }
            case NOT_EQUALS -> {
                return !Objects.equals(value, rule.getFilterValue());
            }
            case NOT_CONTAINS -> {
                return !value.contains(rule.getFilterValue());
            }
            default -> {
                return false;
            }
        }
    }

    @Override
    public Message processing(Message message, Rule[] rules) {

        try {
            JsonNode jsonNode = mapper.readTree(message.getValue());
            for (var rule : rules) {
                JsonNode temp = jsonNode.get(rule.getFieldName());
                if (temp == null)
                {
                    message.setFilterState(false);
                    return message;
                }

                if (!temp.isValueNode())
                {
                    message.setFilterState(false);
                    return message;
                }

                String value = jsonNode.get(rule.getFieldName()).asText();
                if (value == null)
                {
                    message.setFilterState(false);
                    return message;
                }
                if (value.isEmpty())
                {
                    message.setFilterState(false);
                    return message;
                }
                if (!compare(value, rule)) {
                    message.setFilterState(false);
                    return message;
                }

                message.setFilterState(true);
            }
        } catch (Exception e){
            log.error("Json error :{}", e.toString());
        }

        return message;
    }
}
