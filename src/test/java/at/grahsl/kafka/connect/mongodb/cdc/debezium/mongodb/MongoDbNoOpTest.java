package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNull;

@RunWith(JUnitPlatform.class)
public class MongoDbNoOpTest {

    @Test
    @DisplayName("when any cdc event then WriteModel is null resulting in Optional.empty() in the corresponding handler")
    public void testValidSinkDocument() {
        assertAll("test behaviour of MongoDbNoOp",
                () -> assertNull(new MongoDbNoOp().perform(new SinkDocument(null,null)),"MongoDbNoOp must result in null WriteModel"),
                () -> assertNull(new MongoDbNoOp().perform(null),"MongoDbNoOp must result in null WriteModel")
        );
    }

}
