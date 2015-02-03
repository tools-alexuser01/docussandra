package com.strategicgains.docussandra.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;

public class KeyTest
{
	@Test
	public void shouldDetectString()
	{
		String s = "Todd Fredrich";
		Key k = new Key(s);
		assertTrue(k.isType(KeyType.STRING));
		assertEquals(s, k.object());
	}

	@Test
	public void shouldDetectUUID()
	{
		UUID uuid = UUID.randomUUID();
		Key k = new Key(uuid);
		assertTrue(k.isType(KeyType.UUID));
		assertEquals(uuid, k.object());
	}

	@Test
	public void shouldDetectInt32()
	{
		Key k = new Key(32);
		assertTrue(k.isType(KeyType.INT32));
		assertEquals(32, k.object());
	}

	@Test
	public void shouldDetectInteger32()
	{
		Key k = new Key(new Integer(32));
		assertTrue(k.isType(KeyType.INT32));
		assertEquals(32, k.object());
	}

	@Test
	public void shouldDetectInt64()
	{
		Key k = new Key(64l);
		assertTrue(k.isType(KeyType.INT64));
		assertEquals(64l, k.object());
	}

	@Test
	public void shouldDetectInteger64()
	{
		Key k = new Key(new Long(64));
		assertTrue(k.isType(KeyType.INT64));
		assertEquals(64l, k.object());
	}

	@Test(expected=InvalidKeyType.class)
	public void shouldThrowOnInvalidType()
	{
		new Key(new AtomicDouble(3.14159));
	}
}
