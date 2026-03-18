package io.quarkus.apicurio.registry.kafkaconnect;

import io.quarkus.deployment.Feature;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ExtensionSslNativeSupportBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

public class ApicurioRegistryKafkaConnectProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(Feature.APICURIO_REGISTRY_KAFKA_CONNECT);
    }

    @BuildStep
    public void apicurioRegistryKafkaConnect(BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {

        reflectiveClass.produce(ReflectiveClassBuildItem.builder(
                "io.apicurio.registry.utils.converter.AvroConverter",
                "io.apicurio.registry.utils.converter.ExtJsonConverter",
                "io.apicurio.registry.utils.converter.SerdeBasedConverter").methods().build());

        reflectiveClass.produce(ReflectiveClassBuildItem.builder(
                "io.apicurio.registry.utils.converter.json.CompactFormatStrategy",
                "io.apicurio.registry.utils.converter.json.PrettyFormatStrategy",
                "io.apicurio.registry.utils.converter.avro.AvroData",
                "io.apicurio.registry.utils.converter.avro.AvroDataConfig").methods().fields()
                .build());

        reflectiveClass.produce(ReflectiveClassBuildItem.builder(
                "io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy",
                "io.apicurio.registry.serde.strategy.TopicIdStrategy",
                "io.apicurio.registry.serde.Default4ByteIdHandler",
                "io.apicurio.registry.serde.Legacy8ByteIdHandler",
                "io.apicurio.registry.serde.OptimisticFallbackIdHandler",
                "io.apicurio.registry.serde.fallback.DefaultFallbackArtifactProvider",
                "io.apicurio.registry.serde.kafka.headers.DefaultHeadersHandler").methods().fields()
                .build());
    }

    @BuildStep
    ExtensionSslNativeSupportBuildItem enableSslInNative() {
        return new ExtensionSslNativeSupportBuildItem(Feature.APICURIO_REGISTRY_KAFKA_CONNECT);
    }
}
