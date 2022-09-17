package eu.navispeed.rabbitmq;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.FieldNameConstants;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

@Builder
@Value
@FieldNameConstants
public class Configuration {
    @Builder.Default
    String hostname = "guest";
    @Builder.Default
    Integer port = 5672;
    @Builder.Default
    String user = "guest";
    @Builder.Default
    String password = "guest";
    @Builder.Default
    String virtualHost = "/";
    @Builder.Default
    Boolean useSsl = false;

    String queue;

    public static Configuration from(CaseInsensitiveStringMap options) {
        return new ConfigurationBuilder().build();
    }
}
