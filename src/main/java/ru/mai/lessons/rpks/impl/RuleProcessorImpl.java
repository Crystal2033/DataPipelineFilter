package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

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
            throw new RuntimeException(e);
        }

// Accessing the three target data elements
//        Map<String, Object> preDateMap = (Map) map.get("pre-date");
//        System.out.println(preDateMap.get("enable"));
//        System.out.println(preDateMap.get("days"));
//        System.out.println(preDateMap.get("interval"));
        if (!isExit){
            for (int i = 0; i<rules.length; i++){
    //            ПРОВЕРКА ПРАВИЛ
                log.info("CHECKING FIELD {}",rules[i].getFieldName());
                if (map.containsKey(rules[i].getFieldName())){
                    if (Objects.equals(rules[i].getFilterFunctionName(), "equals")){
                        if(!Objects.equals(map.get(rules[i].getFieldName()).toString(), rules[i].getFilterValue())){
                            message.setFilterState(false);
                            log.info("set to false - equals {}", map.get(rules[i].getFieldName()));
                            return message;
                        }
                    }
                    if (Objects.equals(rules[i].getFilterFunctionName(), "not_equals")){
                        if(Objects.equals(map.get(rules[i].getFieldName()), rules[i].getFilterValue())){
                            message.setFilterState(false);
                            log.info("set to false - not equals");
                            return message;
                        }
                    }
                    if (Objects.equals(rules[i].getFilterFunctionName(), "contains")){
                        if(!map.get(rules[i].getFieldName()).toString().contains(rules[i].getFilterValue())){
                            message.setFilterState(false);
                            log.info("set to false - contains");
                            return message;
                        }
                    }
                    if (Objects.equals(rules[i].getFilterFunctionName(), "not_contains")){
                        if(map.get(rules[i].getFieldName()).toString().contains(rules[i].getFilterValue())){
                            message.setFilterState(false);
                            log.info("set to false - not contains");
                            return message;
                        }
                    }
                }
                else{
                    break;
                }

            }

            return message;
        }
        else {
            message.setValue("$exit");
            message.setFilterState(true);
            return message;
        }
    }
}
