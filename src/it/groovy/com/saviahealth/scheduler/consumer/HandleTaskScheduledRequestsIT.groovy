package com.saviahealth.scheduler.consumer

import com.saviahealth.scheduler.WorkflowTaskSchedulerApplication
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.common.policies.data.TenantInfo
import org.intermountain.abd.workflow.common.WorkflowTaskExecutionContextDescriptor
import org.intermountain.abd.workflow.domain.events.WorkflowEventType
import org.intermountain.abd.workflow.domain.events.WorkflowTaskExecutionScheduled
import org.intermountain.abd.workflow.domain.events.WorkflowTaskScheduleTriggerFired
import org.intermountain.abd.workflow.domain.model.WorkflowTaskExecutionDescriptor
import org.intermountain.abd.workflow.domain.model.schedule.WorkflowTaskDefinition
import org.intermountain.abd.workflow.domain.model.schedule.WorkflowTaskSchedule
import org.intermountain.abd.workflow.support.identity.EntityIdentifier
import org.intermountain.abd.workflow.support.provenance.ProvenanceEntry
import org.quartz.JobKey
import org.quartz.Scheduler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.pulsar.core.PulsarTemplate
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

@SpringBootTest()
@ContextConfiguration(classes = [WorkflowTaskSchedulerApplication])
class HandleTaskScheduledRequestsIT extends Specification {
    static final Logger log = LoggerFactory.getLogger(HandleTaskScheduledRequestsIT)
    static final TENANT = 'integration-tests'
    static final NAMESPACE = 'savia'
    static final INBOUND_TOPIC = 'workflow-task-execution-scheduled'
    static final OUTBOUND_TOPIC = 'workflow-task-schedule-trigger-fired'


    static pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.0.0"))
            .withEnv([
                    PULSAR_PREFIX_forceDeleteTenantAllowed   : 'true',
                    PULSAR_PREFIX_forceDeleteNamespaceAllowed: 'true'
            ])
    @Shared
    PulsarContainer pulsar = pulsarContainer
    @Shared
    PulsarAdmin pulsarAdmin

    @Autowired
    PulsarTemplate<byte[]> pulsarTemplate

    @Autowired
    PulsarClient pulsarClient

    @Autowired
    Scheduler scheduler

    @Value('${messaging.scheduler-topic-name}')
    String inboundTopic

    @Value('${messaging.workflow-task-schedule-trigger-fired-topic-name}')
    String outboundTopic

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.pulsar.client.service-url", () -> pulsarContainer.pulsarBrokerUrl);
        registry.add('messaging.tenant', () -> TENANT)
        registry.add('messaging.namespace', () -> NAMESPACE)
        registry.add('messaging.scheduler-topic-name-suffix', () -> INBOUND_TOPIC)
        registry.add('messaging.workflow-task-schedule-trigger-fired-topic-suffix', () -> OUTBOUND_TOPIC)
    }

    def setupSpec() {
        pulsar.start()
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsar.httpServiceUrl).build()
        def clusters = pulsarAdmin.clusters().getClusters().collect { it.toString() }.toSet()
        log.info("Creating tenant $TENANT with clusters $clusters")
        pulsarAdmin.tenants().createTenant(TENANT, TenantInfo.builder().allowedClusters(clusters).build())
        log.info("Creating namespace $TENANT/$NAMESPACE")
        pulsarAdmin.namespaces().createNamespace("$TENANT/$NAMESPACE")
        log.info("Creating topic $TENANT/$NAMESPACE/$INBOUND_TOPIC")
        pulsarAdmin.topics().createNonPartitionedTopic("$TENANT/$NAMESPACE/$INBOUND_TOPIC")
        log.info("Creating topic $TENANT/$NAMESPACE/$OUTBOUND_TOPIC")
        pulsarAdmin.topics().createNonPartitionedTopic("$TENANT/$NAMESPACE/$OUTBOUND_TOPIC")
    }

    def cleanupSpec() {
        pulsar.stop()
    }

    def setup() {
    }

    def cleanup() {
    }


    def "workflow-task-schedule-requested schedules job"() {
        given:
            def outboundReader = pulsarClient.newReader()
                    .topic(outboundTopic)
                    .startMessageId(MessageId.latest)
                    .create()

        when:
            pulsarTemplate.send(inboundTopic, eventJson.bytes)
        then: 'job is scheduled'
            new PollingConditions(timeout: 10, delay: 0.25).eventually({ ->
                scheduler.checkExists(jobKey)
            })
        when:
            def message = outboundReader.readNext(10, TimeUnit.SECONDS)
        then:
            message != null
        when:
            def triggeredEvent = WorkflowTaskScheduleTriggerFired.deserialize(new String(message.data))
        then:
            noExceptionThrown()
            triggeredEvent != null
            triggeredEvent.eventType == WorkflowEventType.WORKFLOW_TASK_SCHEDULE_TRIGGER_FIRED
        where:
            // set well into future to avoid the job triggering before the test completes
            startTime = Instant.now().plus(10, ChronoUnit.SECONDS)
            identity = UUID.randomUUID().toString()
            executionId = createExecutionId('it-test-scope')
            eventJson = createEventJson(executionId, startTime)
            jobKey = JobKey.jobKey(executionId.toString(), WorkflowEventType.WORKFLOW_TASK_EXECUTION_SCHEDULED.name())
    }


    private static createExecutionId(String scope) {
        EntityIdentifier.builder()
                .identifies(WorkflowEventType.WORKFLOW_TASK_EXECUTION_SCHEDULED.name())
                .scope(scope)
                .value('1')
                .build()
    }

    private static createEventJson(EntityIdentifier executionId, Instant scheduleStartTime) {
        def contextDescriptor = WorkflowTaskExecutionContextDescriptor.builder()
                .taskExecutionId(executionId)
                .build()
        def descriptor = WorkflowTaskExecutionDescriptor.builder()
                .taskExecutionContextDescriptor(contextDescriptor)
                .taskDefinition(WorkflowTaskDefinition.builder().build())
                .build()
        WorkflowTaskExecutionScheduled.builder()
                .workflowTaskExecutionDescriptor(descriptor)
                .workflowTaskSchedule(new WorkflowTaskSchedule(scheduleStartTime))
                .build()
                .serialize()
    }
}
