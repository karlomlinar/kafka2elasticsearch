package hr.ib.k2e.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import hr.ib.k2e.dto.MessageLog;
import hr.ib.k2e.dto.MessagePrice;

import java.io.IOException;

public class MessageConverter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public MessagePrice convertToMessagePrice(String priceValue) {
        try {
            return objectMapper.readValue(priceValue, MessagePrice.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MessageLog convertToMessageLog(String logValue) {
        try {
            return objectMapper.readValue(logValue, MessageLog.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
