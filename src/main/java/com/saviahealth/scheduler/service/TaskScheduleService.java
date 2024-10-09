package com.saviahealth.scheduler.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.intermountain.abd.workflow.domain.events.WorkflowEventType;
import org.intermountain.abd.workflow.domain.events.WorkflowTaskExecutionScheduled;
import org.intermountain.abd.workflow.domain.model.schedule.WorkflowTaskSchedule;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Date;

import static com.saviahealth.scheduler.service.ScheduledTaskJob.DataKeys.REQUEST;

@Service
public class TaskScheduleService {

    private static final Logger logger = LoggerFactory.getLogger(TaskScheduleService.class);
    private final Scheduler scheduler;
    private final ObjectMapper objectMapper;

    public TaskScheduleService(Scheduler scheduler, ObjectMapper objectMapper) {
        this.scheduler = scheduler;
        this.objectMapper = objectMapper;
    }

    public void scheduleTask(WorkflowTaskExecutionScheduled workflowTaskScheduleRequested) {
        try {
            String group = WorkflowEventType.WORKFLOW_TASK_EXECUTION_SCHEDULED.name();
            // Define the job and tie it to our Job class
            String executionId = workflowTaskScheduleRequested.getWorkflowTaskExecutionDescriptor()
                    .getTaskExecutionContextDescriptor()
                    .getTaskExecutionId().toString();
            JobDetail job = JobBuilder.newJob(ScheduledTaskJob.class)
                    .withIdentity(executionId, group)
                    .build();

            // Trigger the job to run
            WorkflowTaskSchedule taskSchedule = workflowTaskScheduleRequested.getWorkflowTaskSchedule();
            Trigger trigger = TriggerBuilder.newTrigger()
                    .usingJobData(REQUEST.name(), objectMapper.writeValueAsString(workflowTaskScheduleRequested))
                    .withIdentity(executionId, group)
                    .startAt( taskSchedule.getStartTimeRef() == null ? new Date() : Date.from(taskSchedule.getStartTimeRef()) )
                    .build();

            // Start the scheduler if not already started
            if (!scheduler.isStarted()) {
                scheduler.start();
            }

            // Tell quartz to schedule the job using our trigger
            logger.info("Scheduling jobKey={} to run at jobStartTime='{}'", job.getKey(), trigger.getStartTime());
            scheduler.scheduleJob(job, trigger);

        } catch (SchedulerException se) {
            logger.error("Error scheduling task", se);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error serializing task", e);
        }
    }
}