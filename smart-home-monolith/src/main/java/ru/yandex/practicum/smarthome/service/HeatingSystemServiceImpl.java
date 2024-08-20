package ru.yandex.practicum.smarthome.service;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.smarthome.dto.HeatingSystemDto;
import ru.yandex.practicum.smarthome.entity.HeatingSystem;
import ru.yandex.practicum.smarthome.repository.HeatingSystemRepository;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class HeatingSystemServiceImpl implements HeatingSystemService {

    private static final Logger logger = LoggerFactory.getLogger(HeatingSystemServiceImpl.class);
    private final HeatingSystemRepository heatingSystemRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_TEMPERATURE_REQUEST = "heating-system-temperature-request";
    private static final String TOPIC_TEMPERATURE_RESPONSE = "heating-system-temperature-response";

    // Карта для хранения асинхронных запросов
    private final ConcurrentHashMap<Long, CompletableFuture<Double>> temperatureResponses = new ConcurrentHashMap<>();

    @Override
    public HeatingSystemDto getHeatingSystem(Long id) {
        HeatingSystem heatingSystem = heatingSystemRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("HeatingSystem not found"));
        return convertToDto(heatingSystem);
    }

    @Override
    public HeatingSystemDto updateHeatingSystem(Long id, HeatingSystemDto heatingSystemDto) {
        HeatingSystem existingHeatingSystem = heatingSystemRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("HeatingSystem not found"));
        existingHeatingSystem.setOn(heatingSystemDto.isOn());
        existingHeatingSystem.setTargetTemperature(heatingSystemDto.getTargetTemperature());
        HeatingSystem updatedHeatingSystem = heatingSystemRepository.save(existingHeatingSystem);
        return convertToDto(updatedHeatingSystem);
    }

    @Override
    public void turnOn(Long id) {
        logger.info("Sending turn-on message for heating system with id {}", id);
        kafkaTemplate.send("heating-system-control", id.toString(), "TURN_ON");
    }

    @Override
    public void turnOff(Long id) {
        logger.info("Sending turn-off message for heating system with id {}", id);
        kafkaTemplate.send("heating-system-control", id.toString(), "TURN_OFF");
    }

    @Override
    public void setTargetTemperature(Long id, double temperature) {
        logger.info("Sending set-temperature message for heating system with id {}: {}", id, temperature);
        kafkaTemplate.send("heating-system-set-temperature", id.toString(), temperature);
    }

    @Override
    public Double getCurrentTemperature(Long id) {
        logger.info("Requesting current temperature for heating system with id {}", id);

        // Создаем CompletableFuture для асинхронного получения температуры
        CompletableFuture<Double> futureTemperature = new CompletableFuture<>();
        temperatureResponses.put(id, futureTemperature);

        // Отправляем запрос на получение температуры
        kafkaTemplate.send(TOPIC_TEMPERATURE_REQUEST, id.toString(), "GET_TEMPERATURE");

        try {
            // Ожидаем ответ (например, в течение 5 секунд)
            return futureTemperature.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get current temperature", e);
        } finally {
            // Удаляем запрос после получения ответа или при ошибке
            temperatureResponses.remove(id);
        }
    }

    @KafkaListener(topics = TOPIC_TEMPERATURE_RESPONSE, groupId = "heating-system")
    public void handleTemperatureResponse(String key, Double temperature) {
        Long systemId = Long.valueOf(key);
        CompletableFuture<Double> futureTemperature = temperatureResponses.get(systemId);

        if (futureTemperature != null) {
            // Завершаем CompletableFuture с полученной температурой
            futureTemperature.complete(temperature);
        }
    }

    private HeatingSystemDto convertToDto(HeatingSystem heatingSystem) {
        HeatingSystemDto dto = new HeatingSystemDto();
        dto.setId(heatingSystem.getId());
        dto.setOn(heatingSystem.isOn());
        dto.setTargetTemperature(heatingSystem.getTargetTemperature());
        return dto;
    }
}
