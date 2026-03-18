package io.quarkus.it.kafka.connect;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;

import io.apicurio.registry.utils.converter.AvroConverter;
import io.vertx.core.json.JsonObject;

@Path("/converter")
@ApplicationScoped
public class KafkaConnectConverterEndpoint {

    @org.eclipse.microprofile.config.inject.ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.apicurio.registry.url")
    String registryUrl;

    @GET
    @Path("/registry-url")
    @Produces(MediaType.TEXT_PLAIN)
    public String getRegistryUrl() {
        return registryUrl;
    }

    @POST
    @Path("/avro-roundtrip")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject avroConverterRoundtrip(JsonObject input) {
        Map<String, String> config = new HashMap<>();
        config.put("apicurio.registry.url", registryUrl);
        config.put("apicurio.registry.auto-register", "true");

        AvroConverter converter = new AvroConverter();
        converter.configure(config, false);

        String topic = "test-connect-avro";
        String name = input.getString("name");
        String color = input.getString("color");

        Schema avroSchema = SchemaBuilder.record("Pet")
                .namespace("io.quarkus.it.kafka.connect")
                .fields()
                .requiredString("name")
                .requiredString("color")
                .endRecord();

        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("name", name);
        record.put("color", color);

        io.apicurio.registry.utils.converter.avro.AvroData avroData = new io.apicurio.registry.utils.converter.avro.AvroData(1);
        SchemaAndValue connectData = avroData.toConnectData(avroSchema, record);

        byte[] serialized = converter.fromConnectData(topic, connectData.schema(), connectData.value());

        SchemaAndValue deserialized = converter.toConnectData(topic, serialized);

        org.apache.kafka.connect.data.Struct resultStruct = (org.apache.kafka.connect.data.Struct) deserialized.value();
        JsonObject result = new JsonObject();
        result.put("name", resultStruct.getString("name"));
        result.put("color", resultStruct.getString("color"));

        converter.close();
        return result;
    }

    @POST
    @Path("/json-roundtrip")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject jsonConverterRoundtrip(JsonObject input) {
        Map<String, String> config = new HashMap<>();
        config.put("apicurio.registry.url", registryUrl);
        config.put("apicurio.registry.auto-register", "true");

        io.apicurio.registry.utils.converter.ExtJsonConverter converter = new io.apicurio.registry.utils.converter.ExtJsonConverter();
        converter.configure(config, false);

        String topic = "test-connect-json";

        org.apache.kafka.connect.data.Schema connectSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .name("Pet")
                .field("name", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .field("color", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .build();

        org.apache.kafka.connect.data.Struct connectValue = new org.apache.kafka.connect.data.Struct(connectSchema);
        connectValue.put("name", input.getString("name"));
        connectValue.put("color", input.getString("color"));

        byte[] serialized = converter.fromConnectData(topic, connectSchema, connectValue);

        SchemaAndValue deserialized = converter.toConnectData(topic, serialized);

        org.apache.kafka.connect.data.Struct resultStruct = (org.apache.kafka.connect.data.Struct) deserialized.value();
        JsonObject result = new JsonObject();
        result.put("name", resultStruct.getString("name"));
        result.put("color", resultStruct.getString("color"));

        converter.close();
        return result;
    }
}
