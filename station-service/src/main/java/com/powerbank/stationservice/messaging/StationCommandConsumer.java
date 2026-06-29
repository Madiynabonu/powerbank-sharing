package com.powerbank.stationservice.messaging;

import com.powerbank.stationservice.messaging.event.AcquireLockCommand;
import com.powerbank.stationservice.messaging.event.AcquireLockResult;
import com.powerbank.stationservice.messaging.event.EjectCommand;
import com.powerbank.stationservice.messaging.event.EjectResult;
import com.powerbank.stationservice.messaging.event.ReturnPowerbankCommand;
import com.powerbank.stationservice.service.StationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Consumes station commands from rental-service and replies with results.
 * The simulation delay happens inside {@link StationService} — this keeps
 * the consumer blocked deliberately (models real async IoT latency).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StationCommandConsumer {

    private final StationService stationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.acquire-cabinet-lock-result}")
    private String lockResultTopic;

    @Value("${app.kafka.topics.eject-powerbank-result}")
    private String ejectResultTopic;

    @KafkaListener(
            topics = "${app.kafka.topics.acquire-cabinet-lock}",
            containerFactory = "lockCommandListenerFactory")
    public void onAcquireLock(AcquireLockCommand cmd) {
        log.info("Station received acquire-lock command rental={}", cmd.rentalId());
        AcquireLockResult result = stationService.simulateLock(cmd);
        // key = rentalId for ordered delivery to rental-service
        kafkaTemplate.send(lockResultTopic, cmd.rentalId().toString(), result);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.eject-powerbank}",
            containerFactory = "ejectCommandListenerFactory")
    public void onEject(EjectCommand cmd) {
        log.info("Station received eject command powerbank={} rental={}", cmd.powerbankId(), cmd.rentalId());
        EjectResult result = stationService.simulateEject(cmd);
        kafkaTemplate.send(ejectResultTopic, cmd.rentalId().toString(), result);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.return-powerbank-command}",
            containerFactory = "returnPowerbankListenerFactory")
    public void onReturnPowerbank(ReturnPowerbankCommand cmd) {
        log.info("Station received return-powerbank command powerbank={} station={} rental={}",
                cmd.powerbankId(), cmd.returnStationId(), cmd.rentalId());
        stationService.returnPowerBank(cmd.powerbankId(), cmd.returnStationId());
    }
}
