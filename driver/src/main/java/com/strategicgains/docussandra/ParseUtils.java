/*
 * Copyright 2015 udeyoje.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.strategicgains.docussandra;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.strategicgains.docussandra.exception.IndexParseFieldException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import org.apache.commons.codec.binary.Base64;

/**
 * Utility class for parsing JSON fields to useful java objects.
 *
 * @author udeyoje
 */
public class ParseUtils
{

    public static ByteBuffer convertBase64StringToByteBuffer(String in)
    {
        return ByteBuffer.wrap(Base64.decodeBase64(in.getBytes()));
    }

    public static boolean convertStringToBoolean(String in) throws IndexParseFieldException
    {
        if (in.equalsIgnoreCase("T"))//we could put this whole method in one or so line, but it is more readable this way
        {
            return true;
        } else if (in.equalsIgnoreCase("TRUE"))
        {
            return true;
        } else if (in.equalsIgnoreCase("1"))//byte level?
        {
            return true;
        } else if (in.equalsIgnoreCase("F"))
        {
            return false;
        } else if (in.equalsIgnoreCase("FALSE"))
        {
            return false;
        } else if (in.equalsIgnoreCase("0"))//byte level
        {
            return false;
        }
        throw new IndexParseFieldException(in);
    }

    public static Date convertStringToDate(String in) throws IndexParseFieldException
    {
        Parser parser = new Parser();
        List<DateGroup> dg = parser.parse(in);
        if (dg.isEmpty())
        {
            throw new IndexParseFieldException(in);
        }
        List<Date> dates = dg.get(0).getDates();
        if (dates.isEmpty())
        {
            throw new IndexParseFieldException(in);
        }
        return dates.get(0);
    }
}
