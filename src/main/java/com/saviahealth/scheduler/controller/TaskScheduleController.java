package com.saviahealth.scheduler.controller;

import com.saviahealth.scheduler.service.TaskScheduleService;
import org.intermountain.abd.workflow.domain.events.WorkflowTaskExecutionScheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/schedules")
public class TaskScheduleController {

    private static final Logger logger = LoggerFactory.getLogger(TaskScheduleController.class);
    private final TaskScheduleService taskScheduleService;

    public TaskScheduleController(TaskScheduleService taskScheduleService) {
        this.taskScheduleService = taskScheduleService;
    }

    @PostMapping(
            value = "/tasks",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public WorkflowTaskExecutionScheduled scheduleTask(@RequestBody WorkflowTaskExecutionScheduled task) {
        // Log the input
        logger.info("Received task scheduling request: {}", task);

        // Call the scheduleTask method of TaskScheduleService
        taskScheduleService.scheduleTask(task);

        // Return the scheduled task
        logger.info("Task scheduled");
        return task;
    }
}