package com.saviahealth.scheduler


import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.PropertySource
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

@SpringBootTest(properties = [
        'messaging.enabled=false'
])
@PropertySource("classpath:application.yaml")
@ContextConfiguration(classes = [WorkflowTaskSchedulerApplication])
class ContextLoadTest extends Specification {
    @Value('${messaging.tenant:}')
    String tenant
    @Value('${messaging.namespace:}')
    String namespace
    @Value('${messaging.scheduler-topic-name:}')
    String inboundTopic

    def "context loads"() {
        expect:
            true
            tenant
            namespace
            inboundTopic
    }
}
