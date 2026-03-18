package io.quarkus.it.kafka;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class KafkaConnectConverterTest {

    @Test
    public void testRegistryUrlAvailable() {
        given()
                .when().get("/converter/registry-url")
                .then()
                .statusCode(200)
                .body(endsWith("/apis/registry/v3"));
    }

    @Test
    public void testAvroConverterRoundtrip() {
        given()
                .header("content-type", "application/json")
                .body("{\"name\":\"neo\", \"color\":\"tricolor\"}")
                .when().post("/converter/avro-roundtrip")
                .then()
                .statusCode(200)
                .body("name", is("neo"))
                .body("color", is("tricolor"));
    }

    @Test
    public void testJsonConverterRoundtrip() {
        given()
                .header("content-type", "application/json")
                .body("{\"name\":\"luna\", \"color\":\"black\"}")
                .when().post("/converter/json-roundtrip")
                .then()
                .statusCode(200)
                .body("name", is("luna"))
                .body("color", is("black"));
    }
}
