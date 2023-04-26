package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import org.jooq.tools.json.*;

import java.util.Objects;

@Slf4j
public class RuleProcess implements RuleProcessor {

    private boolean checkingEquals(Object value, String filterValue, String fieldName)
    {
        boolean check;
        if (fieldName.equals("age"))
            check = Objects.equals(value, Long.parseLong(filterValue));
        else
            check = Objects.equals(value, filterValue);
        return check;
    }
    private boolean checkingContains(Object value, String filterValue, String fieldName)
    {
        boolean check = false;
        if (!fieldName.equals("age")) {
            if(value == null)
                check = Objects.equals(null, filterValue);
            else {
                String strValue = (String) value;
                check = strValue.contains(filterValue);
            }
        }
        return check;
    }

    public Message processing(Message message, Rule[] rules) throws ParseException {

        if (rules.length == 0){
            return message;
        }

        String jsonString = message.getValue();
        jsonString =  jsonString.replace(":-", ":null");
        jsonString =  jsonString.replace(":,", ":null,");
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonString);

        int index = 0;
        for (Rule rule : rules)
        {
            String fieldName = rule.getFieldName();
            String filterFunction = rule.getFilterFunctionName();
            String filterValue = rule.getFilterValue();

            var value = jsonObject.get(fieldName);
            log.info("Rule {}:, {} {} {}", index++, fieldName, filterFunction, filterValue);

            boolean check;
            switch (filterFunction) {
                case "equals"  -> check = checkingEquals(value, filterValue, fieldName);
                case "not_equals" -> check = !checkingEquals(value, filterValue, fieldName);
                case "contains" -> check = checkingContains(value, filterValue, fieldName);
                case "not_contains" -> {
                    if (!fieldName.equals("age") && value != null) {
                        String strValue = (String) value;
                        check = !strValue.contains(filterValue);
                    }
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
