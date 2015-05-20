package com.strategicgains.docussandra.serialization;

import java.util.UUID;

import org.restexpress.serialization.json.JacksonJsonProcessor;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.strategicgains.hyperexpress.domain.hal.HalResource;
import com.strategicgains.hyperexpress.serialization.jackson.HalResourceDeserializer;
import com.strategicgains.hyperexpress.serialization.jackson.HalResourceSerializer;

public class JsonSerializationProcessor
        extends JacksonJsonProcessor
{

    @Override
    protected void initializeModule(SimpleModule module)
    {
        super.initializeModule(module);
        module.addDeserializer(UUID.class, new UuidDeserializer());
        module.addSerializer(UUID.class, new UuidSerializer());
        module.addDeserializer(HalResource.class, new HalResourceDeserializer());
        module.addSerializer(HalResource.class, new HalResourceSerializer());
    }
}
