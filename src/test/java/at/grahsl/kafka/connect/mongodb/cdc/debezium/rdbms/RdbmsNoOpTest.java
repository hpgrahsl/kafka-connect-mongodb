package at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNull;

@RunWith(JUnitPlatform.class)
public class RdbmsNoOpTest {

    @Test
    @DisplayName("when any cdc event then WriteModel is null resulting in Optional.empty() in the corresponding handler")
    public void testValidSinkDocument() {
        assertAll("test behaviour of MongoDbNoOp",
                () -> assertNull(new RdbmsNoOp().perform(new SinkDocument(null,null)),"RdbmsNoOp must result in null WriteModel"),
                () -> assertNull(new RdbmsNoOp().perform(null),"RdbmsNoOp must result in null WriteModel")
        );
    }

}
