/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.converter;

import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.*;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.DateFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.DecimalFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.TimeFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.TimestampFieldConverter;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//looks like Avro and JSON + Schema is convertible by means of
//a unified conversion approach since they are using the
//same the Struct/Type information ...
public class AvroJsonSchemafulRecordConverter implements RecordConverter {

    public static final Set<String> LOGICAL_TYPE_NAMES = new HashSet<>(
            Arrays.asList(Date.LOGICAL_NAME, Decimal.LOGICAL_NAME,
                            Time.LOGICAL_NAME, Timestamp.LOGICAL_NAME)
        );

    private final Map<Schema.Type, SinkFieldConverter> converters = new HashMap<>();
    private final Map<String, SinkFieldConverter> logicalConverters = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(AvroJsonSchemafulRecordConverter.class);

    public AvroJsonSchemafulRecordConverter() {

        //standard types
        registerSinkFieldConverter(new BooleanFieldConverter());
        registerSinkFieldConverter(new Int8FieldConverter());
        registerSinkFieldConverter(new Int16FieldConverter());
        registerSinkFieldConverter(new Int32FieldConverter());
        registerSinkFieldConverter(new Int64FieldConverter());
        registerSinkFieldConverter(new Float32FieldConverter());
        registerSinkFieldConverter(new Float64FieldConverter());
        registerSinkFieldConverter(new StringFieldConverter());
        registerSinkFieldConverter(new BytesFieldConverter());

        //logical types
        registerSinkFieldLogicalConverter(new DateFieldConverter());
        registerSinkFieldLogicalConverter(new TimeFieldConverter());
        registerSinkFieldLogicalConverter(new TimestampFieldConverter());
        registerSinkFieldLogicalConverter(new DecimalFieldConverter());
    }

    @Override
    public BsonDocument convert(Schema schema, Object value) {

        if(schema == null || value == null) {
            throw new DataException("error: schema and/or value was null for AVRO conversion");
        }

        return toBsonDoc(schema, value);

    }

    private void registerSinkFieldConverter(SinkFieldConverter converter) {
        converters.put(converter.getSchema().type(), converter);
    }

    private void registerSinkFieldLogicalConverter(SinkFieldConverter converter) {
        logicalConverters.put(converter.getSchema().name(), converter);
    }

    private BsonDocument toBsonDoc(Schema schema, Object value) {
        BsonDocument doc = new BsonDocument();
        schema.fields().forEach(f -> processField(doc, (Struct)value, f));
        return doc;
    }

    private void processField(BsonDocument doc, Struct struct, Field field) {

        logger.trace("processing field '{}'",field.name());

        if(isSupportedLogicalType(field.schema())) {
            doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field),field.schema()));
            return;
        }

        try {
            switch(field.schema().type()) {
                case BOOLEAN:
                case FLOAT32:
                case FLOAT64:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case STRING:
                case BYTES:
                    handlePrimitiveField(doc, struct.get(field), field);
                    break;
                case STRUCT:
                    handleStructField(doc, (Struct)struct.get(field), field);
                    break;
                case ARRAY:
                    doc.put(field.name(),handleArrayField((List)struct.get(field), field));
                    break;
                case MAP:
                    handleMapField(doc, (Map)struct.get(field), field);
                    break;
                default:
                    logger.error("Invalid schema. unexpected / unsupported schema type '"
                                 + field.schema().type() + "' for field '"
                                 + field.name() + "' value='" + struct + "'");
                    throw new DataException("unexpected / unsupported schema type " + field.schema().type());
            }
        } catch (Exception exc) {
            logger.error("Error while processing field. schema type '"
               + field.schema().type() + "' for field '"
               + field.name() + "' value='" + struct + "'");
            throw new DataException("error while processing field " + field.name(), exc);
        }

    }

    private void handleMapField(BsonDocument doc, Map m, Field field) {
        logger.trace("handling complex type 'map'");
        if(m==null) {
            logger.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }
        BsonDocument bd = new BsonDocument();
        for(Object entry : m.keySet()) {
            String key = (String)entry;
            Schema.Type valueSchemaType = field.schema().valueSchema().type();
            if(valueSchemaType.isPrimitive()) {
                bd.put(key, getConverter(field.schema().valueSchema()).toBson(m.get(key),field.schema()));
            } else if (valueSchemaType.equals(Schema.Type.ARRAY)) {
                final Field elementField = new Field(key, 0, field.schema().valueSchema());
                final List list = (List)m.get(key);
                logger.trace("adding array values to {} of type valueSchema={} value='{}'",
                   elementField.name(), elementField.schema().valueSchema(), list);
                bd.put(key, handleArrayField(list, elementField));
            } else {
                bd.put(key, toBsonDoc(field.schema().valueSchema(), m.get(key)));
            }
        }
        doc.put(field.name(), bd);
    }

    private BsonValue handleArrayField(List list, Field field) {
        logger.trace("handling complex type 'array' of types '{}'",
           field.schema().valueSchema().type());
        if(list==null) {
            logger.trace("no array -> adding null");
            return BsonNull.VALUE;
        }
        BsonArray array = new BsonArray();
        Schema.Type st = field.schema().valueSchema().type();
        for(Object element : list) {
            if(st.isPrimitive()) {
                array.add(getConverter(field.schema().valueSchema()).toBson(element,field.schema()));
            } else if(st == Schema.Type.ARRAY) {
                Field elementField = new Field("first", 0, field.schema().valueSchema());
                array.add(handleArrayField((List)element,elementField));
            } else {
                array.add(toBsonDoc(field.schema().valueSchema(), element));
            }
        }
        return array;
    }

    private void handleStructField(BsonDocument doc, Struct struct, Field field) {
        logger.trace("handling complex type 'struct'");
        if(struct!=null) {
            logger.trace(struct.toString());
            doc.put(field.name(), toBsonDoc(field.schema(), struct));
        } else {
            logger.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
        }
    }

    private void handlePrimitiveField(BsonDocument doc, Object value, Field field) {
        logger.trace("handling primitive type '{}' name='{}'",field.schema().type(),field.name());
        doc.put(field.name(), getConverter(field.schema()).toBson(value,field.schema()));
    }

    private boolean isSupportedLogicalType(Schema schema) {

        if(schema.name() == null) {
            return false;
        }

        return LOGICAL_TYPE_NAMES.contains(schema.name());

    }

    private SinkFieldConverter getConverter(Schema schema) {

        SinkFieldConverter converter;

        if(isSupportedLogicalType(schema)) {
            converter = logicalConverters.get(schema.name());
        } else {
            converter = converters.get(schema.type());
        }

        if (converter == null) {
            throw new ConnectException("error no registered converter found for " + schema.type().getName());
        }

        return converter;
    }
}
