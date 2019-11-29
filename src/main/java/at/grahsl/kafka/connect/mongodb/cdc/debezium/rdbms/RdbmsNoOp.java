package at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms;

import at.grahsl.kafka.connect.mongodb.cdc.CdcOperation;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;

public class RdbmsNoOp implements CdcOperation {

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc) {
        return null;
    }

}
