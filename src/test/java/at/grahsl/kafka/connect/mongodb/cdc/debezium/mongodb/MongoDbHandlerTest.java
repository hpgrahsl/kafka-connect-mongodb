package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.cdc.CdcOperation;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.OperationType;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms.RdbmsHandler;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class MongoDbHandlerTest {

    public static final MongoDbHandler MONGODB_HANDLER_DEFAULT_MAPPING =
            new MongoDbHandler(new MongoDbSinkConnectorConfig(new HashMap<>()));

    public static final MongoDbHandler MONGODB_HANDLER_EMPTY_MAPPING =
            new MongoDbHandler(new MongoDbSinkConnectorConfig(new HashMap<>()),
                    new HashMap<>());

    public static final RdbmsHandler MONGODB_HANDLER_NOOP_MAPPING =
            new RdbmsHandler(new MongoDbSinkConnectorConfig(new HashMap<>()),
                    new HashMap<OperationType, CdcOperation>() {{
                        put(OperationType.CREATE,new MongoDbNoOp());
                        put(OperationType.READ,new MongoDbNoOp());
                        put(OperationType.UPDATE,new MongoDbNoOp());
                        put(OperationType.DELETE,new MongoDbNoOp());
                    }});

    @Test
    @DisplayName("verify existing default config from base class")
    public void testExistingDefaultConfig() {
        assertAll(
                () -> assertNotNull(MONGODB_HANDLER_DEFAULT_MAPPING.getConfig(),
                        () -> "default config for handler must not be null"),
                () -> assertNotNull(MONGODB_HANDLER_EMPTY_MAPPING.getConfig(),
                        () -> "default config for handler must not be null")
        );
    }

    @Test
    @DisplayName("when key document missing then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class, () ->
            MONGODB_HANDLER_DEFAULT_MAPPING.handle(new SinkDocument(null,null))
        );
    }

    @Test
    @DisplayName("when key doc contains 'id' field but value is empty then null due to tombstone")
    public void testTombstoneEvent() {
        assertEquals(Optional.empty(),
                MONGODB_HANDLER_DEFAULT_MAPPING.handle(new SinkDocument(
                        new BsonDocument("id",new BsonInt32(1234)),
                            new BsonDocument())
                ),
                "tombstone event must result in Optional.empty()"
        );
    }

    @Test
    @DisplayName("when value doc contains unknown operation type then DataException")
    public void testUnkownCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                new BsonDocument("op",new BsonString("x"))
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER_DEFAULT_MAPPING.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc contains unmapped operation type then DataException")
    public void testUnmappedCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("_id",new BsonInt32(1234)),
                new BsonDocument("op",new BsonString("c"))
                        .append("after",new BsonString("{_id:1234,foo:\"blah\"}"))
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER_EMPTY_MAPPING.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc contains operation type other than string then DataException")
    public void testInvalidCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                new BsonDocument("op",new BsonInt32('c'))
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER_DEFAULT_MAPPING.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc is missing operation type then DataException")
    public void testMissingCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                new BsonDocument("po", BsonNull.VALUE)
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER_DEFAULT_MAPPING.handle(cdcEvent)
        );
    }

    @TestFactory
    @DisplayName("when valid CDC event then correct WriteModel")
    public Stream<DynamicTest> testValidCdcDocument() {

        return Stream.of(
                dynamicTest("test operation "+OperationType.CREATE, () -> {
                            Optional<WriteModel<BsonDocument>> result =
                                MONGODB_HANDLER_DEFAULT_MAPPING.handle(
                                        new SinkDocument(
                                                new BsonDocument("_id",new BsonString("1234")),
                                                new BsonDocument("op",new BsonString("c"))
                                                        .append("after",new BsonString("{_id:1234,foo:\"blah\"}"))
                                        )
                                );
                            assertTrue(result.isPresent());
                            assertTrue(result.get() instanceof ReplaceOneModel,
                                () -> "result expected to be of type ReplaceOneModel");

                }),
                dynamicTest("test operation "+OperationType.READ, () -> {
                    Optional<WriteModel<BsonDocument>> result =
                            MONGODB_HANDLER_DEFAULT_MAPPING.handle(
                                    new SinkDocument(
                                            new BsonDocument("_id",new BsonString("1234")),
                                            new BsonDocument("op",new BsonString("r"))
                                                    .append("after",new BsonString("{_id:1234,foo:\"blah\"}"))
                                    )
                            );
                    assertTrue(result.isPresent());
                    assertTrue(result.get() instanceof ReplaceOneModel,
                            () -> "result expected to be of type ReplaceOneModel");

                }),
                dynamicTest("test operation "+OperationType.UPDATE, () -> {
                    Optional<WriteModel<BsonDocument>> result =
                            MONGODB_HANDLER_DEFAULT_MAPPING.handle(
                                    new SinkDocument(
                                            new BsonDocument("id",new BsonString("1234")),
                                            new BsonDocument("op",new BsonString("u"))
                                                    .append("patch",new BsonString("{\"$set\":{foo:\"blah\"}}"))
                                    )
                            );
                    assertTrue(result.isPresent());
                    assertTrue(result.get() instanceof UpdateOneModel,
                            () -> "result expected to be of type UpdateOneModel");

                }),
                dynamicTest("test operation "+OperationType.DELETE, () -> {
                    Optional<WriteModel<BsonDocument>> result =
                            MONGODB_HANDLER_DEFAULT_MAPPING.handle(
                                    new SinkDocument(
                                            new BsonDocument("id",new BsonString("1234")),
                                            new BsonDocument("op",new BsonString("d"))
                                    )
                            );
                    assertTrue(result.isPresent(), () -> "write model result must be present");
                    assertTrue(result.get() instanceof DeleteOneModel,
                            () -> "result expected to be of type DeleteOneModel");

                })
        );

    }

        @TestFactory
        @DisplayName("when valid cdc operation type then correct MongoDB CdcOperation")
        public Stream<DynamicTest> testValidCdcOpertionTypes() {

        return Stream.of(
                dynamicTest("test operation "+OperationType.CREATE, () ->
                        assertTrue(MONGODB_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op",new BsonString("c")))
                                instanceof MongoDbInsert)
                ),
                dynamicTest("test operation "+OperationType.READ, () ->
                        assertTrue(MONGODB_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op",new BsonString("r")))
                                instanceof MongoDbInsert)
                ),
                dynamicTest("test operation "+OperationType.UPDATE, () ->
                        assertTrue(MONGODB_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op",new BsonString("u")))
                                instanceof MongoDbUpdate)
                ),
                dynamicTest("test operation "+OperationType.DELETE, () ->
                        assertTrue(MONGODB_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op",new BsonString("d")))
                                instanceof MongoDbDelete)
                )
        );

    }

    @TestFactory
    @DisplayName("when valid cdc operation type mapped to NO OP then CdcOperation of type MongoDbNoOp")
    public Stream<DynamicTest> testValidCdcOpertionWithNoOpMappings() {

        return Stream.of(OperationType.values()).map(ot ->
                dynamicTest("test operation " + ot, () ->
                        assertTrue(MONGODB_HANDLER_NOOP_MAPPING.getCdcOperation(
                                new BsonDocument("op", new BsonString("c")))
                                instanceof MongoDbNoOp)
                )
        );

    }

    @TestFactory
    @DisplayName("when valid CDC event with noop mapping then empty WriteModel")
    public Stream<DynamicTest> testValidCdcDocumentWithNoOpMapping() {

        return Stream.of(
                dynamicTest("test operation "+OperationType.CREATE,
                        () -> assertEquals(Optional.empty(),
                                MONGODB_HANDLER_NOOP_MAPPING.handle(
                                        new SinkDocument(
                                                new BsonDocument("_id",new BsonString("1234")),
                                                new BsonDocument("op",new BsonString("c"))
                                                        .append("after",new BsonString("{_id:1234,foo:\"blah\"}"))
                                        )
                                ),
                                () -> "result of MongoDbNoOp must be Optional.empty()")

                ),
                dynamicTest("test operation "+OperationType.READ,
                        () -> assertEquals(Optional.empty(),
                                MONGODB_HANDLER_NOOP_MAPPING.handle(
                                        new SinkDocument(
                                                new BsonDocument("_id",new BsonString("1234")),
                                                new BsonDocument("op",new BsonString("r"))
                                                        .append("after",new BsonString("{_id:1234,foo:\"blah\"}"))
                                        )
                                ),
                                () -> "result of MongoDbNoOp must be Optional.empty()")

                ),
                dynamicTest("test operation "+OperationType.UPDATE,
                        () -> assertEquals(Optional.empty(),
                                MONGODB_HANDLER_NOOP_MAPPING.handle(
                                        new SinkDocument(
                                                new BsonDocument("id",new BsonString("1234")),
                                                new BsonDocument("op",new BsonString("u"))
                                                        .append("patch",new BsonString("{\"$set\":{foo:\"blah\"}}"))
                                        )
                                ),
                                () -> "result of MongoDbNoOp must be Optional.empty()")

                ),
                dynamicTest("test operation "+OperationType.DELETE,
                        () -> assertEquals(Optional.empty(),
                                MONGODB_HANDLER_NOOP_MAPPING.handle(
                                        new SinkDocument(
                                                new BsonDocument("id",new BsonString("1234")),
                                                new BsonDocument("op",new BsonString("d"))
                                        )
                                ),
                                () -> "result of MongoDbNoOp must be Optional.empty()")

                )
        );

    }

}
