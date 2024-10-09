package com.saviahealth.scheduler;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.quartz.SchedulerFactoryBeanCustomizer;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.core.PulsarClientBuilderCustomizer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@SpringBootApplication
public class WorkflowTaskSchedulerApplication {

	public static void main(String[] args) {
		SpringApplication.run(WorkflowTaskSchedulerApplication.class, args);
	}

	@Bean
	RedeliveryBackoff negativeRedeliveryBackoff(
			@Value("${messaging.consumer-nack-redelivery-min-timeout-ms}") long minTimeout,
			@Value("${messaging.consumer-nack-redelivery-max-timeout-ms}") long maxTimeout,
			@Value("${messaging.consumer-nack-redelivery-multiplier}") double multiplier
	) {
		return MultiplierRedeliveryBackoff.builder()
				.minDelayMs(minTimeout)
				.maxDelayMs(maxTimeout)
				.multiplier(multiplier)
				.build();
	}

	@Bean
	public DeadLetterPolicy deadLetterPolicy(
			@Value("${messaging.retries}") int retries,
			@Value("${messaging.dead-letter-topic-name}") String deadLetterTopicName,
			@Value("${messaging.dead-letter-consumer-id}") String deadLetterConsumerId
	) {
		return DeadLetterPolicy.builder()
				.maxRedeliverCount(retries)
				.deadLetterTopic(deadLetterTopicName)
				.retryLetterTopic("")
				.initialSubscriptionName(deadLetterConsumerId)
				.build();
	}

	@Bean
	public SchedulerFactoryBeanCustomizer schedulerFactoryBeanCustomizer(ApplicationContext applicationContext) {
		return schedulerFactoryBean -> {
            AutoWiringSpringBeanJobFactory autoWiringSpringBeanJobFactory = new AutoWiringSpringBeanJobFactory();
            autoWiringSpringBeanJobFactory.setApplicationContext(applicationContext);
            schedulerFactoryBean.setJobFactory(autoWiringSpringBeanJobFactory);
        };
	}
}
