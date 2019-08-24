package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.*;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class WriteModelStrategyTest {

    public static final DeleteOneDefaultStrategy DELETE_ONE_DEFAULT_STRATEGY =
            new DeleteOneDefaultStrategy();

    public static final ReplaceOneDefaultStrategy REPLACE_ONE_DEFAULT_STRATEGY =
            new ReplaceOneDefaultStrategy();

    public static final ReplaceOneBusinessKeyStrategy REPLACE_ONE_BUSINESS_KEY_STRATEGY =
            new ReplaceOneBusinessKeyStrategy();

    public static final UpdateOneTimestampsStrategy UPDATE_ONE_TIMESTAMPS_STRATEGY =
            new UpdateOneTimestampsStrategy();

    public static final MonotonicWritesDefaultStrategy MONOTONIC_WRITES_DEFAULT_STRATEGY =
            new MonotonicWritesDefaultStrategy();

    public static final BsonDocument FILTER_DOC_DELETE_DEFAULT = new BsonDocument(DBCollection.ID_FIELD_NAME,
            new BsonDocument("id",new BsonInt32(1004)));

    public static final BsonDocument FILTER_DOC_REPLACE_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004));

    public static final BsonDocument REPLACEMENT_DOC_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                    .append("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"))
                    .append("email",new BsonString("annek@noanswer.org"));

    public static final BsonDocument FILTER_DOC_REPLACE_BUSINESS_KEY =
            new BsonDocument("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"));

    public static final BsonDocument FILTER_DOC_MONOTONIC_WRITES_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004));

    public static final BsonDocument REPLACEMENT_DOC_BUSINESS_KEY =
            new BsonDocument("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"))
                    .append("email",new BsonString("annek@noanswer.org"))
                    .append("age", new BsonInt32(23))
                    .append("active", new BsonBoolean(true));

    public static final BsonDocument FILTER_DOC_UPDATE_TIMESTAMPS =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004));

    public static final BsonDocument UPDATE_DOC_TIMESTAMPS =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                    .append("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"))
                    .append("email",new BsonString("annek@noanswer.org"));

    public static final BsonDocument MONOTONIC_WRITES_DOC_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                    .append("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"))
                    .append("email",new BsonString("annek@noanswer.org"))
                    .append("_kafkaCoords",new BsonDocument("_topic",new BsonString("some-topic"))
                            .append("_partition",new BsonInt32(1))
                            .append("_offset",new BsonInt64(111))
                    );

    @Test
    @DisplayName("when key document is missing for DeleteOneDefaultStrategy then DataException")
    public void testDeleteOneDefaultStrategyWithMissingKeyDocument() {

        assertThrows(DataException.class,() ->
                DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(null, new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for DeleteOneDefaultStrategy then correct DeleteOneModel")
    public void testDeleteOneDefaultStrategyWitValidSinkDocument() {

        BsonDocument keyDoc = new BsonDocument("id",new BsonInt32(1004));

        WriteModel<BsonDocument> result =
                DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(keyDoc,null));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_DELETE_DEFAULT,writeModel.getFilter());

    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneDefaultStrategy then DataException")
    public void testReplaceOneDefaultStrategyWithMissingValueDocument() {

        assertThrows(DataException.class,() ->
                REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneDefaultStrategy then correct ReplaceOneModel")
    public void testReplaceOneDefaultStrategyWithValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                .append("first_name",new BsonString("Anne"))
                .append("last_name",new BsonString("Kretchmar"))
                .append("email",new BsonString("annek@noanswer.org"));

        WriteModel<BsonDocument> result =
                REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(null,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC_DEFAULT,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_REPLACE_DEFAULT,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneBusinessKeyStrategy then DataException")
    public void testReplaceOneBusinessKeyStrategyWithMissingValueDocument() {

        assertThrows(DataException.class,() ->
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when value document is missing an _id field for ReplaceOneBusinessKeyStrategy then DataException")
    public void testReplaceOneBusinessKeyStrategyWithMissingIdFieldInValueDocument() {

        assertThrows(DataException.class,() ->
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneBusinessKeyStrategy then correct ReplaceOneModel")
    public void testReplaceOneBusinessKeyStrategyWithValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME,
                new BsonDocument("first_name",new BsonString("Anne"))
                        .append("last_name",new BsonString("Kretchmar")))
                .append("first_name",new BsonString("Anne"))
                .append("last_name",new BsonString("Kretchmar"))
                .append("email",new BsonString("annek@noanswer.org"))
                .append("age", new BsonInt32(23))
                .append("active", new BsonBoolean(true));

        WriteModel<BsonDocument> result =
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(new SinkDocument(null,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC_BUSINESS_KEY,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_REPLACE_BUSINESS_KEY,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when value document is missing for UpdateOneTimestampsStrategy then DataException")
    public void testUpdateOneTimestampsStrategyWithMissingValueDocument() {

        assertThrows(DataException.class,() ->
                UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for UpdateOneTimestampsStrategy then correct UpdateOneModel")
    public void  testUpdateOneTimestampsStrategyWithValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                .append("first_name",new BsonString("Anne"))
                .append("last_name",new BsonString("Kretchmar"))
                .append("email",new BsonString("annek@noanswer.org"));

        WriteModel<BsonDocument> result =
                UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(new SinkDocument(null,valueDoc));

        assertTrue(result instanceof UpdateOneModel,
                () -> "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel =
                (UpdateOneModel<BsonDocument>) result;

        //NOTE: This test case can only check:
        // i) for both fields to be available
        // ii) having the correct BSON type (BsonDateTime)
        // iii) and be initially equal
        // The exact dateTime value is not directly testable here.
        BsonDocument updateDoc = (BsonDocument)writeModel.getUpdate();

        BsonDateTime modifiedTS = updateDoc.getDocument("$set")
                                    .getDateTime(UpdateOneTimestampsStrategy.FIELDNAME_MODIFIED_TS);
        BsonDateTime insertedTS = updateDoc.getDocument("$setOnInsert")
                .getDateTime(UpdateOneTimestampsStrategy.FIELDNAME_INSERTED_TS);

        assertTrue(insertedTS.equals(modifiedTS),
                () -> "modified and inserted timestamps must initially be equal");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_UPDATE_TIMESTAMPS,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "update expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when value document is missing for MonotonicWritesDefaultStrategy then DataException")
    public void  testMonotonicWritesDefaultStrategyWithMissingValueDocument() {

        assertThrows(DataException.class,() ->
                MONOTONIC_WRITES_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null),null
                )
        );

    }

    @Test
    @DisplayName("when sink record param is missing for MonotonicWritesDefaultStrategy then DataException")
    public void  testMonotonicWritesDefaultStrategyWithMissingSinkRecord() {

        assertThrows(DataException.class,() ->
                MONOTONIC_WRITES_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when sink document and sink record is valid for MonotonicWritesDefaultStrategy then correct UpdateOneModel")
    public void testMonotonicWritesDefaultStrategyWithValidSinkDocumentAndSinkRecord() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                .append("first_name",new BsonString("Anne"))
                .append("last_name",new BsonString("Kretchmar"))
                .append("email",new BsonString("annek@noanswer.org"));

        SinkRecord sinkRecord = new SinkRecord("some-topic",1,null,null,null,null,111);

        WriteModel<BsonDocument> result =
                MONOTONIC_WRITES_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(null,valueDoc),sinkRecord);

        assertTrue(result instanceof UpdateOneModel,
                () -> "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel =
                (UpdateOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_MONOTONIC_WRITES_DEFAULT,writeModel.getFilter());

        List<? extends Bson> updatePipeline = writeModel.getUpdatePipeline();

        assertNotNull(updatePipeline,() -> "update pipeline must not be null");
        assertEquals(1, updatePipeline.size(),
                () -> "update pipeline expected to contain exactly 1 element only");

        BsonDocument first = (BsonDocument)updatePipeline.get(0);

        BsonDocument condDoc = first.get("$replaceRoot").asDocument()
                .get("newRoot").asDocument()
                .get("$cond").asDocument();

        String condIfP1Name = condDoc.get("if").asDocument().get("$and").asArray()
                .get(0).asDocument().get("$eq").asArray()
                .get(0).asString().getValue();

        String condIfP1Value = condDoc.get("if").asDocument().get("$and").asArray()
                .get(0).asDocument().get("$eq").asArray()
                .get(1).asString().getValue();

        assertEquals("$$ROOT."+ MonotonicWritesDefaultStrategy.FIELD_KAFKA_COORDS
                + "." + MonotonicWritesDefaultStrategy.FIELD_TOPIC,condIfP1Name);
        assertEquals(sinkRecord.topic(),condIfP1Value);

        String condIfP2Name =  condDoc.get("if").asDocument().get("$and").asArray()
                .get(1).asDocument().get("$eq").asArray()
                .get(0).asString().getValue();

        Integer condIfP2Value = condDoc.get("if").asDocument().get("$and").asArray()
                .get(1).asDocument().get("$eq").asArray()
                .get(1).asInt32().getValue();

        assertEquals("$$ROOT."+ MonotonicWritesDefaultStrategy.FIELD_KAFKA_COORDS
                + "." + MonotonicWritesDefaultStrategy.FIELD_PARTITION,condIfP2Name);
        assertEquals(sinkRecord.kafkaPartition(),condIfP2Value);

        String condIfP3Name =  condDoc.get("if").asDocument().get("$and").asArray()
                .get(2).asDocument().get("$gte").asArray()
                .get(0).asString().getValue();

        Long condIfP3Value = condDoc.get("if").asDocument().get("$and").asArray()
                .get(2).asDocument().get("$gte").asArray()
                .get(1).asInt64().getValue();

        assertEquals("$$ROOT."+ MonotonicWritesDefaultStrategy.FIELD_KAFKA_COORDS
                + "." + MonotonicWritesDefaultStrategy.FIELD_OFFSET,condIfP3Name);
        assertEquals(sinkRecord.kafkaOffset(),condIfP3Value);

        String condThen = condDoc.get("then").asString().getValue();

        assertEquals("$$ROOT",condThen,
                () -> "conditional then branch must be $$ROOT");

        BsonDocument condElseDoc = condDoc.get("else").asDocument();

        assertEquals(MONOTONIC_WRITES_DOC_DEFAULT,condElseDoc,
                ()-> "conditional else branch contains wrong update document");

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

}
