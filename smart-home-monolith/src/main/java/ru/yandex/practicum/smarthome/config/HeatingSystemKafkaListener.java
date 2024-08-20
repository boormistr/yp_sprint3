package ru.yandex.practicum.smarthome.kafka;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.smarthome.service.HeatingSystemService;

@Component
@RequiredArgsConstructor
public class HeatingSystemKafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(HeatingSystemKafkaListener.class);
    private final HeatingSystemService heatingSystemService;

    @KafkaListener(topics = "heating-system-turn-on", groupId = "heating-group")
    public void listenTurnOn(String id) {
        logger.info("Received turn-on message for heating system with id {}", id);
        heatingSystemService.turnOn(Long.parseLong(id));
    }

    @KafkaListener(topics = "heating-system-turn-off", groupId = "heating-group")
    public void listenTurnOff(String id) {
        logger.info("Received turn-off message for heating system with id {}", id);
        heatingSystemService.turnOff(Long.parseLong(id));
    }

    @KafkaListener(topics = "heating-system-set-temperature", groupId = "heating-group")
    public void listenSetTemperature(String id, double temperature) {
        logger.info("Received set-temperature message for heating system with id {}: {}", id, temperature);
        heatingSystemService.setTargetTemperature(Long.parseLong(id), temperature);
    }
}
