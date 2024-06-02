package streaming.models;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class Review {
    public Long userId;
    public Long productId;
    public Double review;
    public Long timestamp;
}