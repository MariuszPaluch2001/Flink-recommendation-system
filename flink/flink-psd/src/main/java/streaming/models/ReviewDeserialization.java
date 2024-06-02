package streaming.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class ReviewDeserialization implements
        DeserializationSchema < Review > {

    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Review deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Review.class);
    }

    @Override
    public boolean isEndOfStream(Review review) {
        return false;
    }

    @Override
    public TypeInformation < Review > getProducedType() {
        return TypeInformation.of(Review.class);
    }
}