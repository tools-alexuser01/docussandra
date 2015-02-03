/*
    Copyright 2015, Strategic Gains, Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package com.strategicgains.docussandra.domain;

/**
 * These types indicate the underlying structure of the binary (blob) data in a Key.
 * 
 * @author toddf
 * @since Feb 1, 2015
 */
public enum KeyType
{
	// a java.lang.String.
	STRING((byte) 0x00),

	// a java.util.Date
	TIMESTAMP((byte) 0x01),

	// an int or Integer
	INT32((byte) 0x02),

	// a long or Long
	INT64((byte) 0x03),

	// a UUID
	UUID((byte) 0x04);

	// Not implemented yet.
//	TimeUUID((byte) 0x05);

	private final byte value;

	private KeyType(byte value)
	{
		this.value = value;
	}

	public byte value()
	{
		return value;
	}

	public static KeyType valueOf(byte value)
	{
		if (value == 0x00) return STRING;
		if (value == 0x01) return TIMESTAMP;
		if (value == 0x02) return INT32;
		if (value == 0x03) return INT64;
		if (value == 0x04) return UUID;
//		if (value == 0x05) return TimeUUID;

		throw new InvalidKeyType(String.format("%x", value));
	}
}
