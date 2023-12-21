package io.quarkus.deployment;

import io.quarkus.deployment.builditem.FeatureBuildItem;

/**
 * Represents a feature provided by a core extension.
 *
 * @see FeatureBuildItem
 */
public enum Feature {

    AGROAL,
    AMAZON_LAMBDA,
    AZURE_FUNCTIONS,
    APICURIO_REGISTRY_AVRO,
    APICURIO_REGISTRY_JSON_SCHEMA,
    APICURIO_REGISTRY_PROTOBUF,
    AWT,
    CACHE,
    CDI,
    CONFIG_YAML,
    CONFLUENT_REGISTRY_AVRO,
    CONFLUENT_REGISTRY_JSON,
    ELASTICSEARCH_REST_CLIENT_COMMON,
    ELASTICSEARCH_REST_CLIENT,
    ELASTICSEARCH_REST_HIGH_LEVEL_CLIENT,
    ELASTICSEARCH_JAVA_CLIENT,
    FLYWAY,
    GRPC_CLIENT,
    GRPC_SERVER,
    HIBERNATE_ORM,
    HIBERNATE_ENVERS,
    HIBERNATE_ORM_PANACHE,
    HIBERNATE_ORM_PANACHE_KOTLIN,
    HIBERNATE_ORM_REST_DATA_PANACHE,
    HIBERNATE_REACTIVE,
    HIBERNATE_REACTIVE_PANACHE,
    HIBERNATE_REACTIVE_PANACHE_KOTLIN,
    HIBERNATE_REACTIVE_REST_DATA_PANACHE,
    HIBERNATE_SEARCH_ELASTICSEARCH,
    HIBERNATE_VALIDATOR,
    INFINISPAN_CLIENT,
    INFINISPAN_EMBEDDED,
    JDBC_DB2,
    JDBC_DERBY,
    JDBC_H2,
    JDBC_POSTGRESQL,
    JDBC_MARIADB,
    JDBC_MSSQL,
    JDBC_MYSQL,
    JDBC_ORACLE,
    KAFKA_CLIENT,
    KAFKA_STREAMS,
    KEYCLOAK_AUTHORIZATION,
    KOTLIN,
    KUBERNETES,
    KUBERNETES_CLIENT,
    LIQUIBASE,
    LIQUIBASE_MONGODB,
    LOGGING_GELF,
    MAILER,
    MICROMETER,
    MONGODB_CLIENT,
    MONGODB_PANACHE,
    MONGODB_PANACHE_KOTLIN,
    MONGODB_REST_DATA_PANACHE,
    MUTINY,
    NARAYANA_JTA,
    NARAYANA_LRA,
    NARAYANA_STM,
    NEO4J,
    OIDC,
    OIDC_CLIENT,
    OIDC_CLIENT_FILTER,
    OIDC_CLIENT_REACTIVE_FILTER,
    OIDC_CLIENT_GRAPHQL_CLIENT_INTEGRATION,
    OIDC_TOKEN_PROPAGATION,
    OIDC_TOKEN_PROPAGATION_REACTIVE,
    OPENSHIFT_CLIENT,
    OPENTELEMETRY,
    OPENTELEMETRY_JAEGER_EXPORTER,
    OPENTELEMETRY_OTLP_EXPORTER,
    PICOCLI,
    QUARTZ,
    QUTE,
    REACTIVE_PG_CLIENT,
    REACTIVE_MYSQL_CLIENT,
    REACTIVE_MSSQL_CLIENT,
    REACTIVE_DB2_CLIENT,
    REACTIVE_ORACLE_CLIENT,
    REACTIVE_ROUTES,
    REDIS_CLIENT,
    RESTEASY,
    RESTEASY_JACKSON,
    RESTEASY_JAXB,
    RESTEASY_JSONB,
    RESTEASY_MULTIPART,
    RESTEASY_MUTINY,
    RESTEASY_QUTE,
    RESTEASY_REACTIVE,
    RESTEASY_REACTIVE_QUTE,
    RESTEASY_REACTIVE_JSONB,
    RESTEASY_REACTIVE_JAXB,
    RESTEASY_REACTIVE_JACKSON,
    RESTEASY_REACTIVE_KOTLIN_SERIALIZATION,
    RESTEASY_REACTIVE_LINKS,
    RESTEASY_CLIENT,
    RESTEASY_CLIENT_JACKSON,
    RESTEASY_CLIENT_JAXB,
    RESTEASY_CLIENT_JSONB,
    RESTEASY_CLIENT_MUTINY,
    REST_CLIENT_REACTIVE,
    REST_CLIENT_REACTIVE_JACKSON,
    REST_CLIENT_REACTIVE_JAXB,
    REST_CLIENT_REACTIVE_JSONB,
    REST_CLIENT_REACTIVE_KOTLIN_SERIALIZATION,
    SCALA,
    SCHEDULER,
    SECURITY,
    SECURITY_JDBC,
    SECURITY_LDAP,
    SECURITY_JPA,
    SECURITY_JPA_REACTIVE,
    SECURITY_PROPERTIES_FILE,
    SECURITY_OAUTH2,
    SECURITY_WEBAUTHN,
    SERVLET,
    SMALLRYE_CONTEXT_PROPAGATION,
    SMALLRYE_FAULT_TOLERANCE,
    SMALLRYE_HEALTH,
    SMALLRYE_JWT,
    SMALLRYE_METRICS,
    SMALLRYE_OPENAPI,
    SMALLRYE_REACTIVE_MESSAGING,
    SMALLRYE_REACTIVE_MESSAGING_KAFKA,
    SMALLRYE_REACTIVE_MESSAGING_AMQP,
    SMALLRYE_REACTIVE_MESSAGING_MQTT,
    SMALLRYE_REACTIVE_MESSAGING_RABBITMQ,
    SMALLRYE_REACTIVE_MESSAGING_PULSAR,
    SMALLRYE_REACTIVE_STREAMS_OPERATORS,
    SMALLRYE_REACTIVE_TYPE_CONVERTERS,
    SMALLRYE_GRAPHQL,
    SMALLRYE_GRAPHQL_CLIENT,
    SPRING_DI,
    SPRING_WEB,
    SPRING_DATA_JPA,
    SPRING_DATA_REST,
    SPRING_SECURITY,
    SPRING_BOOT_PROPERTIES,
    SPRING_CACHE,
    SPRING_CLOUD_CONFIG_CLIENT,
    SPRING_SCHEDULED,
    SWAGGER_UI,
    WEBSOCKETS,
    WEBSOCKETS_CLIENT,
    VAULT,
    VERTX,
    VERTX_GRAPHQL,
    WEBJARS_LOCATOR;

    public String getName() {
        return toString().toLowerCase().replace('_', '-');
    }

}
