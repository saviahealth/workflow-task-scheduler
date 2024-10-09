package com.saviahealth.scheduler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.intermountain.abd.workflow.domain.events.WorkflowTaskExecutionScheduled;
import org.intermountain.abd.workflow.domain.events.WorkflowTaskScheduleTriggerFired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

@Service
public class WorkflowTaskTriggerFhiredPublisherService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowTaskTriggerFhiredPublisherService.class);

    private static final String ENTITY_IDENTIFIER_SCOPE = "TASK_SCHEDULER_ENTITY_IDENTIFIER_SCOPE";
    private static final String IDENTIFIER_QUALIFIER = "WorkflowTaskSchedulerService";

    private final PulsarTemplate<byte[]> pulsarTemplate;
    private final ObjectMapper objectMapper;
    private final String workflowTaskScheduleTriggerFiredTopicName;

    public WorkflowTaskTriggerFhiredPublisherService(
            PulsarTemplate<byte[]> pulsarTemplate,
            ObjectMapper objectMapper,
            @Value("${messaging.workflow-task-schedule-trigger-fired-topic-name}") String workflowTaskScheduleTriggerFiredTopicName) {
        this.pulsarTemplate = pulsarTemplate;
        this.objectMapper = objectMapper;
        this.workflowTaskScheduleTriggerFiredTopicName = workflowTaskScheduleTriggerFiredTopicName;
    }

    public void publishWorkflowTaskScheduleTriggerFired(String workflowTaskExecutionScheduled) {
        try {
            WorkflowTaskExecutionScheduled request = objectMapper.readValue(workflowTaskExecutionScheduled, WorkflowTaskExecutionScheduled.class);

            WorkflowTaskScheduleTriggerFired workflowTaskScheduleTriggerFired = WorkflowTaskScheduleTriggerFired.builder()
                    .emitter(IDENTIFIER_QUALIFIER)
                    .eventScope(request.getEventScope())
                    .provenanceSource(request)
                    .taskExecutionDescriptor(request.getWorkflowTaskExecutionDescriptor())
                    .build();

            byte[] outboundEvent = objectMapper.writeValueAsBytes(workflowTaskScheduleTriggerFired);
            pulsarTemplate.send(workflowTaskScheduleTriggerFiredTopicName, outboundEvent);
        } catch (Exception e) {
            throw new RuntimeException("Unable to publish message to Pulsar", e);
        }
    }
}
