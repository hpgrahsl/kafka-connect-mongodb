package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.*;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This WriteModelStrategy implementation adds the kafka coordinates of processed
 * records to the actual SinkDocument as meta-data before it gets written to the
 * MongoDB collection. The pre-defined and currently not(!) configurable data format
 * for this is using a sub-document with the following structure, field names
 * and value &lt;PLACEHOLDERS&gt;:
 *
 * {
 *      ...,
 *
 *      "_kafkaCoords":{
 *   "_topic": "&lt;TOPIC_NAME&gt;",
 *   "_partition": &lt;PARTITION_NUMBER&gt;,
 *   "_offset": &lt;OFFSET_NUMBER&gt;
 *      },
 *
 *      ...
 * }
 *
 * This "meta-data" is used to perform the actual staleness check, namely, that upsert operations
 * based on the corresponding document's _id field will get suppressed in case newer data has
 * already been written to the collection in question. Newer data means a document exhibiting
 * a greater than or equal offset for the same kafka topic and partition is already present in the sink.
 *
 * ! IMPORTANT NOTE !
 * This WriteModelStrategy needs MongoDB version 4.2+ and Java Driver 3.11+ since
 * lower versions of either lack the support for leveraging update pipeline syntax.
 *
 */
public class MonotonicWritesDefaultStrategy implements WriteModelStrategy {

    public static final String FIELD_KAFKA_COORDS = "_kafkaCoords";
    public static final String FIELD_TOPIC = "_topic";
    public static final String FIELD_PARTITION = "_partition";
    public static final String FIELD_OFFSET = "_offset";

    private static final UpdateOptions UPDATE_OPTIONS =
            new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {
        throw new DataException("error: the write model strategy " + MonotonicWritesDefaultStrategy.class.getName()
                + " needs the SinkRecord's data and thus cannot work on the SinkDocument param alone."
                + " please use the provided method overloading for this."
        );
    }

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document, SinkRecord record) {

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        //1) add kafka coordinates to the value document
        //NOTE: future versions might allow to configure the fieldnames
        //via external configuration properties, for now this is pre-defined.
        vd.append(FIELD_KAFKA_COORDS, new BsonDocument(
                FIELD_TOPIC, new BsonString(record.topic()))
                .append(FIELD_PARTITION, new BsonInt32(record.kafkaPartition()))
                .append(FIELD_OFFSET, new BsonInt64(record.kafkaOffset()))
        );

        //2) build the conditional update pipeline based on Kafka coordinates
        //which makes sure that in case records get replayed - e.g. either due to
        //uncommitted offsets or newly started connectors with different names -
        //that stale data never overwrites newer data which was previously written
        //to the sink already.
        List<Bson> conditionalUpdatePipeline = new ArrayList<>();
        conditionalUpdatePipeline.add(new BsonDocument("$replaceRoot",
                new BsonDocument("newRoot", new BsonDocument("$cond",
                        new BsonDocument("if", new BsonDocument("$and",
                                new BsonArray(Arrays.asList(
                                        new BsonDocument("$eq", new BsonArray(Arrays.asList(
                                                new BsonString("$$ROOT." + FIELD_KAFKA_COORDS + "." + FIELD_TOPIC),
                                                new BsonString(record.topic())))),
                                        new BsonDocument("$eq", new BsonArray(Arrays.asList(
                                                new BsonString("$$ROOT." + FIELD_KAFKA_COORDS + "." + FIELD_PARTITION),
                                                new BsonInt32(record.kafkaPartition())))),
                                        new BsonDocument("$gte", new BsonArray(Arrays.asList(
                                                new BsonString("$$ROOT." + FIELD_KAFKA_COORDS + "." + FIELD_OFFSET),
                                                new BsonInt64(record.kafkaOffset()))))
                                ))))
                                .append("then", new BsonString("$$ROOT"))
                                .append("else", vd)
                ))
        ));

        return new UpdateOneModel<>(
                new BsonDocument(DBCollection.ID_FIELD_NAME, vd.get(DBCollection.ID_FIELD_NAME)),
                conditionalUpdatePipeline,
                UPDATE_OPTIONS
        );
    }
}
