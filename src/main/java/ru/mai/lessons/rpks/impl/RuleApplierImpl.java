package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.mai.lessons.rpks.RuleApplier;
import ru.mai.lessons.rpks.exception.ServerException;
import ru.mai.lessons.rpks.model.Rule;

import java.util.stream.Stream;

@Slf4j
public class RuleApplierImpl implements RuleApplier {
    private final ObjectMapper objectMapper;

    public RuleApplierImpl() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public boolean apply(ConsumerRecord<String, String> consumerRecord, Rule[] rules) {
        log.info("Check rules for record {}", consumerRecord.value());
        if (rules == null || rules.length == 0) {
            return false;
        }
        ObjectNode objectNode;
        try {
            objectNode = objectMapper.readValue(consumerRecord.value(), new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            log.error("Не смогли создать json", e);
            return false;
        }

        return Stream.of(rules).allMatch(r -> apply(objectNode, r));
    }

    private boolean apply(ObjectNode objectNode, Rule rule) {
        var fieldNameNode = objectNode.get(rule.getFieldName());
        if (fieldNameNode == null) {
            return false;
        }
        var fieldValue = fieldNameNode.asText();
        return switch (rule.getFilterFunctionName()) {
            case equals -> fieldValue.equals(rule.getFilterValue());
            case not_equals -> !fieldValue.equals(rule.getFilterValue());
            case contains -> fieldValue.contains(rule.getFilterValue());
            case not_contains -> !fieldValue.contains(rule.getFilterValue());
        };
    }
}
