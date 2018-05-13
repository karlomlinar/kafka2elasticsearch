package hr.ib.k2e.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import hr.ib.k2e.dto.MessageLog;
import hr.ib.k2e.dto.MessagePrice;

import java.io.IOException;

/**
 * Class used for converting {@link String} into a POJO
 */

public class MessageConverter {

    /**
     * Class used for converting {@link String} into a POJO
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Converts a json {@link String} to {@link MessagePrice} object
     * @param priceValue {@link String} to convert
     * @return Converted {@link MessagePrice} object
     */
    public MessagePrice convertToMessagePrice(String priceValue) {
        try {
            return objectMapper.readValue(priceValue, MessagePrice.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts a json {@link String} to {@link MessageLog} object
     * @param logValue {@link String} to convert
     * @return Converted {@link MessageLog} object
     */
    public MessageLog convertToMessageLog(String logValue) {
        try {
            return objectMapper.readValue(logValue, MessageLog.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
