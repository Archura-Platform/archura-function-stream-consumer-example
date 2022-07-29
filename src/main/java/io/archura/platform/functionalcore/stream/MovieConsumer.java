package io.archura.platform.functionalcore.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.Configurable;
import io.archura.platform.api.type.functionalcore.StreamConsumer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class MovieConsumer implements StreamConsumer, Configurable {

    private Map<String, Object> configuration;

    @Override
    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void consume(Context context, byte[] key, byte[] value) {
        try {
            final Logger logger = context.getLogger();
            final ObjectMapper objectMapper = context.getObjectMapper();
            final Movie movie = objectMapper.readValue(value, Movie.class);
            logger.info("key: %s, value: %s", new String(key), movie);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
