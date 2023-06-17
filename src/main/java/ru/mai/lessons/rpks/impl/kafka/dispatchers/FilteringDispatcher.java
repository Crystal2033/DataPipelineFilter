package ru.mai.lessons.rpks.impl.kafka.dispatchers;

import com.typesafe.config.Config;
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

import static ru.mai.lessons.rpks.impl.services.ServiceFiltering.KAFKA_NAME;
import static ru.mai.lessons.rpks.impl.services.ServiceFiltering.TOPIC_NAME_PATH;


@Slf4j
@RequiredArgsConstructor
public class FilteringDispatcher {

    private final Config config;

    private final RulesUpdaterThread updaterRulesThread;
    private List<Rule> rulesList;
    private KafkaWriterImpl kafkaWriter;

    public void updateRules() throws ThreadWorkerNotFoundException {
        rulesList = Optional.ofNullable(updaterRulesThread).
                orElseThrow(() -> new ThreadWorkerNotFoundException("Database updater not found")).getRules();
    }

    public void sendMessageIfCompatibleWithDBRules(String checkingMessage) throws UndefinedOperationException, ThreadWorkerNotFoundException {
        kafkaWriter = Optional.ofNullable(kafkaWriter).orElseGet(this::createKafkaWriterForSendingMessage);
        updateRules();
        if (rulesList.isEmpty()) {
            kafkaWriter.processing(getMessage(checkingMessage, false));
            return;
        }
        try {
            JSONObject jsonObject = new JSONObject(checkingMessage);
            for (Rule rule : rulesList) {
                boolean isCompatible = isCompatibleWithRule(rule.getFilterFunctionName(),
                        rule.getFilterValue(), jsonObject.get(rule.getFieldName()).toString());
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
        try {
            OperationName operationName = OperationName.valueOf(operation.toUpperCase());
            return switch (operationName) {
                case EQUALS -> expected.equals(userValue);
                case NOT_EQUALS -> !expected.equals(userValue);
                case CONTAINS -> userValue.contains(expected);
                case NOT_CONTAINS -> !userValue.contains(expected);
            };
        } catch (IllegalArgumentException ex) {
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
        Config producerKafkaConfig = config.getConfig(KAFKA_NAME).getConfig("producer");
        return KafkaWriterImpl.builder()
                .topic(producerKafkaConfig.getConfig("deduplication").getString(TOPIC_NAME_PATH))
                .bootstrapServers(producerKafkaConfig.getString("bootstrap.servers"))
                .build();
    }
}
