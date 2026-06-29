package com.powerbank.paymentservice.config;

import com.powerbank.paymentservice.messaging.event.CancelPaymentCommand;
import com.powerbank.paymentservice.messaging.event.CardCommand;
import com.powerbank.paymentservice.messaging.event.PaymentRequest;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka wiring. Cross-service events travel as plain JSON with NO type headers,
 * so each service can deserialize into its own local record class even though
 * package names differ. Each listener factory pins the concrete target type.
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.payment-result}")
    private String paymentResultTopic;

    @Value("${app.kafka.topics.payment-events}")
    private String paymentEventsTopic;

    @Value("${app.kafka.topics.payment-request}")
    private String paymentRequestTopic;

    @Value("${app.kafka.topics.card-command}")
    private String cardCommandTopic;

    @Value("${app.kafka.topics.cancel-payment-command}")
    private String cancelPaymentCommandTopic;

    // ---------------------------------------------------------------- producer

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // acks=all + idempotent producer => no silent loss / no duplicates on retry
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        // keep payloads provider-agnostic across services
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> pf) {
        return new KafkaTemplate<>(pf);
    }

    // ---------------------------------------------------------------- consumer

    private <T> ConsumerFactory<String, T> consumerFactory(Class<T> type) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-service");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, type.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> listenerFactory(
            Class<T> type, KafkaTemplate<String, Object> template) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(type));
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.RECORD);
        // poison messages: retry a few times, then route to <topic>.DLT instead of blocking the partition
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentRequest> paymentRequestListenerFactory(
            KafkaTemplate<String, Object> template) {
        return listenerFactory(PaymentRequest.class, template);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CardCommand> cardCommandListenerFactory(
            KafkaTemplate<String, Object> template) {
        return listenerFactory(CardCommand.class, template);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CancelPaymentCommand> cancelPaymentListenerFactory(
            KafkaTemplate<String, Object> template) {
        return listenerFactory(CancelPaymentCommand.class, template);
    }

    // ------------------------------------------------------------------ topics

    @Bean
    public org.apache.kafka.clients.admin.NewTopic paymentRequestTopic() {
        return TopicBuilder.name(paymentRequestTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic paymentResultTopic() {
        return TopicBuilder.name(paymentResultTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic paymentEventsTopic() {
        return TopicBuilder.name(paymentEventsTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic cardCommandTopic() {
        return TopicBuilder.name(cardCommandTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic cancelPaymentCommandTopic() {
        return TopicBuilder.name(cancelPaymentCommandTopic).partitions(3).replicas(1).build();
    }
}
