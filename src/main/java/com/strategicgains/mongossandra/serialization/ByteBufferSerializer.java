package com.strategicgains.mongossandra.serialization;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class ByteBufferSerializer
extends JsonSerializer<ByteBuffer>
{
	@Override
	public void serialize(ByteBuffer object, JsonGenerator json, SerializerProvider provider)
	throws IOException, JsonProcessingException
	{
		json.writeString(new String(object.array()));
	}
}
