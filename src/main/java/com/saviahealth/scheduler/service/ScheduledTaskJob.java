package com.saviahealth.scheduler.service;

import org.intermountain.abd.workflow.domain.events.WorkflowTaskExecutionScheduled;
import org.intermountain.abd.workflow.domain.events.WorkflowTaskScheduleTriggerFired;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Component;

@Component
public class ScheduledTaskJob implements Job {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledTaskJob.class);
    private static final String EVENT_EMITTER_NAME = "WorkflowTaskSchedulerService";

    @Autowired
    PulsarTemplate<byte[]> pulsarTemplate;

    @Value("${messaging.workflow-task-schedule-trigger-fired-topic-name}")
    private String workflowTaskScheduleTriggerFiredTopicName;



    public ScheduledTaskJob() {
        // this is used by Spring with field injection
    }

    public ScheduledTaskJob(PulsarTemplate<byte[]> pulsarTemplate, @Value("${messaging.workflow-task-schedule-trigger-fired-topic-name}") String workflowTaskScheduleTriggerFiredTopicName) {
        // This is provided for testing purposes to allow for easier mocking
        this.pulsarTemplate = pulsarTemplate;
        this.workflowTaskScheduleTriggerFiredTopicName = workflowTaskScheduleTriggerFiredTopicName;
    }

    public enum DataKeys {
        REQUEST
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String jobEventParam = (String) context.getMergedJobDataMap().get(DataKeys.REQUEST.name());
        logger.info("ScheduledTaskJob fired!");

        WorkflowTaskExecutionScheduled workflowTaskExecutionScheduled = WorkflowTaskExecutionScheduled.deserialize(jobEventParam);

        WorkflowTaskScheduleTriggerFired workflowTaskScheduleTriggerFired = WorkflowTaskScheduleTriggerFired.builder()
                .emitter(EVENT_EMITTER_NAME)
                .eventScope(workflowTaskExecutionScheduled.getEventScope())
                .provenanceSource(workflowTaskExecutionScheduled)
                .taskExecutionDescriptor(workflowTaskExecutionScheduled.getWorkflowTaskExecutionDescriptor())
                .build();
        pulsarTemplate.send(workflowTaskScheduleTriggerFiredTopicName, workflowTaskScheduleTriggerFired.serialize().getBytes());
    }
}