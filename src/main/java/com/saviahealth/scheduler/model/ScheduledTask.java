package com.saviahealth.scheduler.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.time.ZonedDateTime;

public record ScheduledTask(
        @JsonFormat(shape = JsonFormat.Shape.STRING) ZonedDateTime startTime,
        Integer iterations,
        Integer delay) {
}
