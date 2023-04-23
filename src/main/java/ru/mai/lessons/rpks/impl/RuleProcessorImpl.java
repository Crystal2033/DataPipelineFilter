package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.model.MyException;

import java.util.Map;
import java.util.Objects;
@Slf4j

public class RuleProcessorImpl implements RuleProcessor{
    boolean isExit = false;
    @Override
    public Message processing(Message message, Rule[] rules) {
        ObjectMapper mapper = new ObjectMapper();
        if(Objects.equals(message.getValue(), "$exit")){
            isExit = true;
        }
        Map<String, Object> map;
        message.setFilterState(true);

// To put all of the JSON in a Map<String, Object>
        try {
            map = mapper.readValue(message.getValue(), Map.class);
        } catch (JsonProcessingException e) {
            throw new MyException("Error while parsing json", e);
        }

        if (!isExit){
            for (Rule rule : rules) {
                //            ПРОВЕРКА ПРАВИЛ
                log.info("CHECKING FIELD {}", rule.getFieldName());
                if (map.containsKey(rule.getFieldName())) {
                    if (Objects.equals(rule.getFilterFunctionName(), "equals") && (!Objects.equals(map.get(rule.getFieldName()).toString(), rule.getFilterValue()))) {
                        message.setFilterState(false);
                        log.info("set to false - equals {}", map.get(rule.getFieldName()));
                        return message;

                    }
                    if (Objects.equals(rule.getFilterFunctionName(), "not_equals") && (Objects.equals(map.get(rule.getFieldName()), rule.getFilterValue()))) {
                        message.setFilterState(false);
                        log.info("set to false - not equals");
                        return message;

                    }
                    if (Objects.equals(rule.getFilterFunctionName(), "contains") && (!map.get(rule.getFieldName()).toString().contains(rule.getFilterValue()))) {
                        message.setFilterState(false);
                        log.info("set to false - contains");
                        return message;

                    }
                    if (Objects.equals(rule.getFilterFunctionName(), "not_contains") && (map.get(rule.getFieldName()).toString().contains(rule.getFilterValue()))) {
                        message.setFilterState(false);
                        log.info("set to false - not contains");
                        return message;

                    }
                } else {
                    break;
                }

            }

        }
        else {
            message.setValue("$exit");
            message.setFilterState(true);
        }
        return message;
    }
}