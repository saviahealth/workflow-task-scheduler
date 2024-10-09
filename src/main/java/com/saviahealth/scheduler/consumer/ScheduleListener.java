package com.saviahealth.scheduler.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saviahealth.scheduler.service.TaskScheduleService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.intermountain.abd.workflow.domain.events.WorkflowTaskExecutionScheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@ConditionalOnProperty(prefix = "messaging", name = "enabled", havingValue = "true")
public class ScheduleListener {
    private static final Logger log = LoggerFactory.getLogger(ScheduleListener.class);
    private final ObjectMapper objectMapper;
    private final TaskScheduleService scheduleService;

    public ScheduleListener(ObjectMapper objectMapper, TaskScheduleService scheduleService) {
        this.objectMapper = objectMapper;
        this.scheduleService = scheduleService;
    }

    @PulsarListener(
            subscriptionName = "${messaging.consumerId}"
            , topics = "${messaging.scheduler-topic-name}"
            , subscriptionType = SubscriptionType.Key_Shared
            , ackMode = AckMode.MANUAL
            , schemaType = SchemaType.BYTES
            , deadLetterPolicy = "deadLetterPolicy"
            , negativeAckRedeliveryBackoff = "negativeRedeliveryBackoff"
            , properties = {
                "ackTimeoutMillis=${messaging.consumer-ack-timeout-mills}"
                , "receiverQueueSize=${messaging.consumer-receiver-queue-size}"
                }
    )
    public void consume(Message<byte[]> pulsarMessage, Acknowledgement ack) {
        log.info("Received messageId={} from topic={} of size={}", pulsarMessage.getMessageId(), pulsarMessage.getTopicName(), pulsarMessage.getData().length);

        // Process the message
        try {
            WorkflowTaskExecutionScheduled task = objectMapper.readValue(pulsarMessage.getData(), WorkflowTaskExecutionScheduled.class);
            scheduleService.scheduleTask(task);
        } catch (IOException e) {
            log.error("Error processing messageId={}", pulsarMessage.getMessageId(), e);
            ack.nack();
        }

        ack.acknowledge();
    }
}
