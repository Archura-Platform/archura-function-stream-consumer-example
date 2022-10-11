package io.archura.platform.functionalcore.stream;

import io.archura.platform.api.context.Context;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.Configurable;
import io.archura.platform.api.type.functionalcore.StreamConsumer;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class MovieConsumer implements StreamConsumer, Configurable {

    private Map<String, Object> configuration;

    @Override
    public void setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void consume(Context context, String key, Map<String, String> value) {
        final Logger logger = context.getLogger();
        logger.info("configuration: %s", configuration);

        final Movie movie = new Movie(value.get("title"), Integer.parseInt(value.get("year")));
        logger.info("key: %s, value: %s", key, movie);
    }

}
