package streaming.models;

import java.util.Set;

public class KafkaOutput {
    public Long userId;
    public Set<String> recommendedProducts;

    public KafkaOutput(Long userId, Set<String> recommendedProducts) {
        this.userId = userId;
        this.recommendedProducts = recommendedProducts;
    }
}
