package ru.mai.lessons.rpks.impl.kafka.dispatchers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.exceptions.ThreadWorkerNotFoundException;
import ru.mai.lessons.rpks.exceptions.UndefinedOperationException;
import ru.mai.lessons.rpks.impl.kafka.KafkaWriterImpl;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.OperationName;
import ru.mai.lessons.rpks.model.Rule;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@RequiredArgsConstructor
public class FilteringDispatcher{
    private final String topicToSendMsg;
    private final String bootstrapServers;

    private final RulesUpdaterThread updaterRulesThread;
    private ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;
    private KafkaWriterImpl kafkaWriter;

    public void updateRules() throws ThreadWorkerNotFoundException {
        rulesConcurrentMap = Optional.ofNullable(updaterRulesThread).
                orElseThrow(()->new ThreadWorkerNotFoundException("Database updater not found")).getRulesConcurrentMap();
    }

    public void closeReadingThread() {
        updaterRulesThread.stopReadingDataBase();
    }


    private boolean checkField(String fieldName, JSONObject jsonObject) throws UndefinedOperationException {
        boolean isPassedAllChecks = true;
        try{
            String userValue = jsonObject.get(fieldName).toString();
            List<Rule> rules = rulesConcurrentMap.get(fieldName);
            for (var rule : rules) {
                if (!isCompatibleWithRule(rule.getFilterFunctionName(), rule.getFilterValue(), userValue)) {
                    isPassedAllChecks = false;
                    break;
                }
            }
        }
        catch (JSONException ex){
            return false;
        }
        return isPassedAllChecks;
    }

    public void sendMessageIfCompatibleWithDBRules(String checkingMessage) throws UndefinedOperationException, ThreadWorkerNotFoundException {
        kafkaWriter = Optional.ofNullable(kafkaWriter).orElseGet(this::createKafkaWriterForSendingMessage);
        updateRules();
        if (rulesConcurrentMap.isEmpty()) {
            kafkaWriter.processing(getMessage(checkingMessage, false));
            return;
        }
        try {
            JSONObject jsonObject = new JSONObject(checkingMessage);
            for (String fieldName : rulesConcurrentMap.keySet()){
                boolean isCompatible = checkField(fieldName, jsonObject);
                log.info("compatible after name: {}", isCompatible);
                if (!isCompatible) {
                    kafkaWriter.processing(getMessage(checkingMessage, false));
                    return;
                }
            }
            kafkaWriter.processing(getMessage(checkingMessage, true));
        } catch (JSONException ex) {
            log.error("Parsing json error: ", ex);
        }
    }

    private boolean isCompatibleWithRule(String operation, String expected, String userValue) throws UndefinedOperationException {
        log.info("operation={}, expected={}, userValue={}", operation, expected, userValue);
        try{
            OperationName operationName = OperationName.valueOf(operation.toUpperCase());
            return switch (operationName) {
                case EQUALS -> expected.equals(userValue);
                case NOT_EQUALS -> !expected.equals(userValue);
                case CONTAINS -> userValue.contains(expected);
                case NOT_CONTAINS -> !userValue.contains(expected);
            };
        }
        catch (IllegalArgumentException ex){
            throw new UndefinedOperationException("Operation was not found.", operation);
        }
    }

    private Message getMessage(String value, boolean isCompatible) {
        return Message.builder()
                .value(value)
                .filterState(isCompatible)
                .build();
    }

    private KafkaWriterImpl createKafkaWriterForSendingMessage() {
        return KafkaWriterImpl.builder()
                .topic(topicToSendMsg)
                .bootstrapServers(bootstrapServers)
                .build();
    }
}
