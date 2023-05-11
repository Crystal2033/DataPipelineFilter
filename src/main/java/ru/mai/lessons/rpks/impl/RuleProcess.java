package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import org.jooq.tools.json.*;

import java.util.Objects;

@Slf4j
public class RuleProcess implements RuleProcessor {
    enum enumFunction {EQUALS, NOT_EQUALS, CONTAINS, NOT_CONTAINS}

    public Message processing(Message message, Rule[] rules) throws ParseException {

        if (rules.length == 0){
            return message;
        }

        String jsonString = message.getValue();
        jsonString =  jsonString.replace(":-", ":null");
        jsonString =  jsonString.replace(":,", ":null,");
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonString);

        for (Rule rule : rules)
        {
            String fieldName = rule.getFieldName();
            enumFunction filterFunction = enumFunction.valueOf(rule.getFilterFunctionName().toUpperCase());
            String filterValue = rule.getFilterValue();

            var value = jsonObject.get(fieldName);
            String strValue = String.valueOf(value);

            boolean check;
            switch (filterFunction) {
                case EQUALS -> check = Objects.equals(strValue, filterValue);
                case NOT_EQUALS -> check = !Objects.equals(strValue, filterValue);
                case CONTAINS -> check = strValue.contains(filterValue);
                case NOT_CONTAINS -> {
                    if (value != null) check = !strValue.contains(filterValue);
                    else check = false;
                }
                default -> check = false;
            }

            if(!check) return message;
        }

        message.setFilterState(true);
        return message;
    }
}
