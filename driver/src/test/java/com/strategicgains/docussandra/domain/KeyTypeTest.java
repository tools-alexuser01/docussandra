package com.strategicgains.docussandra.domain;

import static org.junit.Assert.*;

import org.junit.Test;

public class KeyTypeTest
{
	@Test
	public void shouldBeString()
	{
		assertTrue(KeyType.STRING.equals(KeyType.valueOf(KeyType.STRING.value())));
		assertTrue(KeyType.STRING.equals(KeyType.valueOf("STRING")));
	}

	@Test
	public void shouldBeInt32()
	{
		assertTrue(KeyType.INT32.equals(KeyType.valueOf(KeyType.INT32.value())));
		assertTrue(KeyType.INT32.equals(KeyType.valueOf("INT32")));
	}

	@Test
	public void shouldBeInt64()
	{
		assertTrue(KeyType.INT64.equals(KeyType.valueOf(KeyType.INT64.value())));
		assertTrue(KeyType.INT64.equals(KeyType.valueOf("INT64")));
	}

	@Test
	public void shouldBeUuid()
	{
		assertTrue(KeyType.UUID.equals(KeyType.valueOf(KeyType.UUID.value())));
		assertTrue(KeyType.UUID.equals(KeyType.valueOf("UUID")));
	}
}
