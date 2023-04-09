package ru.mai.lessons.rpks.impl.kafka.dispatchers;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.exceptions.UndefinedOperationException;
import ru.mai.lessons.rpks.impl.kafka.KafkaWriterImpl;
import ru.mai.lessons.rpks.impl.repository.RulesUpdaterThread;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.json.*;

@Slf4j
public class FilteringDispatcher implements DispatcherKafka{
    private final String topicToSendMsg;
    private final String bootstrapServers;

    private final ConcurrentLinkedQueue<String> queueConcurrent = new ConcurrentLinkedQueue<>();

    private final RulesUpdaterThread updaterRulesThread;
    private ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;
    private KafkaWriterImpl kafkaWriter;

    public FilteringDispatcher(String topicToSendMsg, String kafkaTopicBootstrap, RulesUpdaterThread updaterRulesThread){
        this.topicToSendMsg = topicToSendMsg;
        this.bootstrapServers = kafkaTopicBootstrap;
        this.updaterRulesThread = updaterRulesThread;
        updateRules();
        kafkaWriter = createKafkaWriterForSendingMessage();
    }

    public void updateRules(){
        if(updaterRulesThread != null){
            rulesConcurrentMap = updaterRulesThread.getRulesConcurrentMap();
        }
    }
    @Override
    public void actionWithMessage(String msg) throws UndefinedOperationException {
        queueConcurrent.add(msg);
        sendMessageIfCompatibleWithDBRules(queueConcurrent.poll());
    }

    public void closeReadingThread(){
        updaterRulesThread.stopReadingDataBase();
    }

    private boolean checkField(String fieldName, JSONObject jsonObject) throws UndefinedOperationException {
        if(rulesConcurrentMap.containsKey(fieldName)){
            String userValue = jsonObject.getString(fieldName);
            List<Rule> rules = rulesConcurrentMap.get(fieldName);
            for(var rule : rules){
                if(!isCompatibleWithRule(rule.getFilterFunctionName(), rule.getFilterValue(), userValue)){
                    return false;
                }
            }
            return true;
        }
        return true;
    }
    private void sendMessageIfCompatibleWithDBRules(String checkingMessage) throws UndefinedOperationException {
        updateRules();
        JSONObject jsonObject = new JSONObject(checkingMessage);

        boolean isCompatible = checkField("name", jsonObject);
        log.info("compatible after name: {}", isCompatible);
        if(!isCompatible){
            kafkaWriter.processing(getMessage(checkingMessage, false));
            return;
        }
        isCompatible = checkField("age", jsonObject);
        log.info("compatible after age: {}", isCompatible);
        if(!isCompatible){
            kafkaWriter.processing(getMessage(checkingMessage, false));
            return;
        }

        isCompatible = checkField("sex", jsonObject);
        log.info("compatible after sex: {}", isCompatible);
        if(!isCompatible){
            kafkaWriter.processing(getMessage(checkingMessage, false));
            return;
        }
        kafkaWriter.processing(getMessage(checkingMessage, true));

    }

    private boolean isCompatibleWithRule(String operation, String expected, String userValue) throws UndefinedOperationException {
        log.info("operation={}, expected={}, userValue={}", operation, expected, userValue);
        return switch (operation) {
            case "equals" -> expected.equals(userValue);
            case "not equals" -> !expected.equals(userValue);
            case "contains" -> userValue.contains(expected);
            case "not contains" -> !userValue.contains(expected);
            default -> throw new UndefinedOperationException("Operation was not found.", operation);
        };
    }
    private Message getMessage(String value, boolean isCompatible){
        return Message.builder()
                .value(value)
                .filterState(isCompatible)
                .build();
    }
    private KafkaWriterImpl createKafkaWriterForSendingMessage(){
        return KafkaWriterImpl.builder()
                .topic(topicToSendMsg)
                .bootstrapServers(bootstrapServers)
                .build();
    }
}
