package hr.ib.k2e.util

import com.fasterxml.jackson.databind.ObjectMapper
import hr.ib.k2e.dto.MessageLog
import hr.ib.k2e.dto.MessagePrice
import spock.lang.Specification

class MessageConverterSpecification extends Specification {

    MessageConverter messageConverter = new MessageConverter()
    ObjectMapper objectMapper = new ObjectMapper()

    def 'Converting a MessageLog String to MessageLog test'() {

        given: 'a MessageLog String to deserialize'
            String messageLog = objectMapper.writeValueAsString(new MessageLog("2", 3, "4", false, 2))

        when: 'a given value is converted'
            def result = messageConverter.convertToMessageLog(messageLog)

        then: 'a result should be of type MessageLog'
            result instanceof MessageLog
    }

    def 'Converting a random String to MessageLog test'() {

        given: 'a random String to deserialize'
            String randomString = "random string"

        when: 'a given value is converted to MessageLog'
            messageConverter.convertToMessageLog(randomString)

        then: 'a RuntimeException should be thrown'
            thrown(RuntimeException)
    }

    def 'Converting a MessagePrice String to MessagePrice test'() {

        given: 'a MessageLog String to deserialize'
            String messagePrice = objectMapper.writeValueAsString(new MessagePrice("2", 4, "2", 5, false))

        when: 'a given value is converted'
            def result = messageConverter.convertToMessagePrice(messagePrice)

        then: 'a result should be of type MessageLog'
            result instanceof MessagePrice
    }

    def 'Converting a random String to MessagePrice test'() {

        given: 'a random String to deserialize'
            String randomString = "random string"

        when: 'a given value is converted to MessageLog'
            messageConverter.convertToMessagePrice(randomString)

        then: 'a RuntimeException should be thrown'
            thrown(RuntimeException)
    }



}
