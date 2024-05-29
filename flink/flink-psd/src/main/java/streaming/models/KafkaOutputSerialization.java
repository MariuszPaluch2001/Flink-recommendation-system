package streaming.models;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaOutputSerialization implements  SerializationSchema<KafkaOutput>{
    private ObjectMapper mapper;

    @Override
    public byte[] serialize(KafkaOutput element) {
            byte[] b = null;
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                b = mapper.writeValueAsBytes(element);
            } catch (JsonProcessingException e) {
                // TODO
            }
            return b;
    }
}
