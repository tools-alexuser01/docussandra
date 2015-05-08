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
import com.strategicgains.util.date.DateAdapter;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;

/**
 * Utility class for parsing JSON field values to useful java objects.
 *
 * @author udeyoje
 */
public class ParseUtils
{

    /**
     * Converts a base64 encoded string to a ByteBuffer.
     *
     * @param in Base64 encoded string.
     * @return A ByteBuffer containing the decoded string.
     */
    public static ByteBuffer parseBase64StringAsByteBuffer(String in)
    {
        return ByteBuffer.wrap(Base64.decodeBase64(in.getBytes()));
    }

    /**
     * Converts a string to a boolean.
     *
     * @param in String to convert to a boolean.
     * @return a boolean representation of the string.
     * @throws IndexParseFieldException if there is no way to determine if the
     * String should be interpreted as true or false.
     */
    public static boolean parseStringAsBoolean(String in) throws IndexParseFieldException
    {
        in = in.trim();
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
        } else if (in.equalsIgnoreCase("0"))//byte level?
        {
            return false;
        }
        throw new IndexParseFieldException(in);
    }

    /**
     * Converts a string to a date. Uses DateAdaptorJ, if that fails, falls back
     * to Natty.
     *
     * @param in String to convert to a date.
     * @return A date based on the string.
     * @throws IndexParseFieldException If the field cannot be parsed as a date.
     */
    public static Date parseStringAsDate(String in) throws IndexParseFieldException //TODO: come back to this and add more tests, i am not yet entirely happy with it
    {
        DateAdapter adapter = new DateAdapter();
        try
        {
            return adapter.parse(in);
        } catch (ParseException e)//fall back to netty if that fails
        {
            Parser parser = new Parser(TimeZone.getTimeZone("GMT"));//assume all dates are GMT
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
            return dates.get(0);//dang; that actually works
        }
    }

    /**
     * Parses a String as a double.
     *
     * @param in String to parse as a double.
     * @return A double that is based on the passed in String.
     * @throws IndexParseFieldException if the String cannot be parsed as a
     * double.
     */
    public static double parseStringAsDouble(String in) throws IndexParseFieldException
    {
        try
        {
            return Double.parseDouble(in);
        } catch (NumberFormatException e)
        {
            throw new IndexParseFieldException(in, e);
        }
    }

    /**
     * Parses a String as a int.
     *
     * @param in String to parse as an int.
     * @return A int that is based on the passed in String.
     * @throws IndexParseFieldException if the String cannot be parsed as an
     * int.
     */
    public static int parseStringAsInt(String in) throws IndexParseFieldException
    {
        try
        {
            return Integer.parseInt(in);
        } catch (NumberFormatException e)
        {
            throw new IndexParseFieldException(in, e);
        }
    }

    /**
     * Parses a String as a UUID.
     *
     * @param in String to parse as an UUID.
     * @return A UUID that is based on the passed in String.
     * @throws IndexParseFieldException if the String cannot be parsed as an
     * UUID.
     */
    public static UUID parseStringAsUUID(String in) throws IndexParseFieldException
    {
        try
        {
            return UUID.fromString(in);
        } catch (IllegalArgumentException e)
        {
            throw new IndexParseFieldException(in, e);
        }
    }
}
