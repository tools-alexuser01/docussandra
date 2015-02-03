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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author toddf
 * @since Feb 1, 2015
 */
public class Key
{
	private KeyType type;
	private Object object;

	public Key(Object o)
	{
		super();
		this.object = o;
		this.type = typeOf(o);
	}

	public boolean isType(KeyType type)
	{
		return this.type == type;
	}

	public Object object()
	{
		return object;
	}

	@Override
	public boolean equals(Object that)
	{
		return true;
	}

	@Override
	public int hashCode()
	{
		return 0;
	}

	public String asJson()
	{
		return String.format("{\"t\":%d,\"o\":\"%s\"}", type, object.toString());
	}

	private KeyType typeOf(Object o)
    {
		if (o instanceof UUID)
		{
			return KeyType.UUID;
		}

		if (o instanceof String)
		{
			return KeyType.STRING;
		}

		if (o instanceof Date)
		{
			return KeyType.TIMESTAMP;
		}

		if (o instanceof Long)
		{
			return KeyType.INT64;
		}

		if (o instanceof Integer)
		{
			return KeyType.INT32;
		}

		throw new InvalidKeyType(o.getClass().getName());
    }
}
