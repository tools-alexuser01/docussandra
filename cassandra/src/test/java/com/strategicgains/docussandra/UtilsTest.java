package com.strategicgains.docussandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.strategicgains.docussandra.domain.FieldDataType;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexField;
import com.strategicgains.docussandra.domain.IndexIdentifier;
import com.strategicgains.docussandra.exception.IndexParseException;
import com.strategicgains.docussandra.testhelper.Fixtures;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author udeyoje
 */
public class UtilsTest
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public UtilsTest()
    {
    }

    @BeforeClass
    public static void setUpClass()
    {
    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of calculateITableName method, of class Utils.
     */
    @Test
    public void testCalculateITableName_3args()
    {
        System.out.println("calculateITableName");
        String databaseName = "myDb";
        String tableName = "myTable";
        String indexName = "yoIndex";
        String expResult = "mydb_mytable_yoindex";
        String result = Utils.calculateITableName(databaseName, tableName, indexName);
        assertEquals(expResult, result);
    }

    /**
     * Test of calculateITableName method, of class Utils.
     */
    @Test
    public void testCalculateITableName_Index()
    {
        System.out.println("calculateITableName");
        String databaseName = "myDb";
        String tableName = "myTable";
        String indexName = "yoIndex";
        Index index = new Index(indexName);
        index.setTable(databaseName, tableName);
        String expResult = "mydb_mytable_yoindex";
        String result = Utils.calculateITableName(index);
        assertEquals(expResult, result);
    }

    /**
     * Test of calculateITableName method, of class Utils.
     */
    @Test
    public void testCalculateITableName_IndexIdentifier()
    {
        System.out.println("calculateITableName");
        IndexIdentifier indexId = new IndexIdentifier(Fixtures.createTestIndexOneField().getId());
        String expResult = "mydb_mytable_myindexwithonefield";
        String result = Utils.calculateITableName(indexId);
        assertEquals(expResult, result);
    }

    /**
     * Test of convertStringToUUID method, of class Utils.
     */
    @Test
    public void testConvertStringToUUID()
    {
        System.out.println("convertStringToUUID");
        String s = "this is a test string";
        UUID result = Utils.convertStringToFuzzyUUID(s);
        UUID result2 = Utils.convertStringToFuzzyUUID(s);
        assertEquals(result2, result);//make sure it produces the same string twice
        assertEquals(UUID.fromString("74686973-2069-7320-0000-000000000000"), result);//make sure we can confirm we are equal to a set value
        s = "this is a sorta long test string!!!~!#$%^&*()_++`1234567890-=[]\\{}|;':<>?,\"qwertyuiopasdfghjklzxcvbnm";
        UUID result3 = Utils.convertStringToFuzzyUUID(s);
        UUID result4 = Utils.convertStringToFuzzyUUID(s);
        assertEquals(result3, result4);//make sure it produces the same string twice
        assertEquals(UUID.fromString("74686973-2069-7320-0000-000000000000"), result3);//make sure we can confirm we are equal to a set value
        //note! UUIDs are equal as the first 8 bytes are the same
    }

    /**
     * Test of convertStringToUUID method, of class Utils. Tests UUID ordering.
     * This test isn't so much of a unit test as a proof of concept.
     */
    @Test
    public void testConvertStringToUUIDOrdering()
    {
        System.out.println("convertStringToUUIDOrdering");
        String a = "a";
        String b = "b";
        String c = "c";
        String z = "z";
        UUID aUUID = Utils.convertStringToFuzzyUUID(a);
        UUID bUUID = Utils.convertStringToFuzzyUUID(b);
        UUID cUUID = Utils.convertStringToFuzzyUUID(c);
        UUID zUUID = Utils.convertStringToFuzzyUUID(z);
        logger.info(a + ": " + aUUID.toString() + " " + aUUID.getMostSignificantBits());
        logger.info(b + ": " + bUUID.toString() + " " + bUUID.getMostSignificantBits());
        logger.info(c + ": " + cUUID.toString() + " " + cUUID.getMostSignificantBits());
        logger.info(z + ": " + zUUID.toString() + " " + zUUID.getMostSignificantBits());

        assertTrue(aUUID.getMostSignificantBits() < bUUID.getMostSignificantBits());
        assertTrue(bUUID.getMostSignificantBits() < cUUID.getMostSignificantBits());
        assertTrue(cUUID.getMostSignificantBits() < zUUID.getMostSignificantBits());
    }

    /**
     * Test of convertStringToUUID method, of class Utils. Tests UUID ordering.
     * This test isn't so much of a unit test as a proof of concept.
     */
    @Test
    public void testConvertStringToUUIDOrdering2()
    {
        System.out.println("convertStringToUUIDOrdering2");
        String a = "apple";
        String b = "baseball";
        String c = "cat";
        String z = "zuul";
        UUID aUUID = Utils.convertStringToFuzzyUUID(a);
        UUID bUUID = Utils.convertStringToFuzzyUUID(b);
        UUID cUUID = Utils.convertStringToFuzzyUUID(c);
        UUID zUUID = Utils.convertStringToFuzzyUUID(z);
        logger.info(a + ": " + aUUID.toString() + " " + aUUID.getMostSignificantBits());
        logger.info(b + ": " + bUUID.toString() + " " + bUUID.getMostSignificantBits());
        logger.info(c + ": " + cUUID.toString() + " " + cUUID.getMostSignificantBits());
        logger.info(z + ": " + zUUID.toString() + " " + zUUID.getMostSignificantBits());

        assertTrue(aUUID.getMostSignificantBits() < bUUID.getMostSignificantBits());
        assertTrue(bUUID.getMostSignificantBits() < cUUID.getMostSignificantBits());
        assertTrue(cUUID.getMostSignificantBits() < zUUID.getMostSignificantBits());
    }

    /**
     * Test of convertStringToUUID method, of class Utils. Tests UUID ordering.
     * This test isn't so much of a unit test as a proof of concept.
     */
    @Test
    public void testConvertStringToUUIDOrdering3()
    {
        System.out.println("convertStringToUUIDOrdering3");
        String a = "adam";
        String b = "alexandria";
        String c = "apartment";
        String z = "apple";
        UUID aUUID = Utils.convertStringToFuzzyUUID(a);
        UUID bUUID = Utils.convertStringToFuzzyUUID(b);
        UUID cUUID = Utils.convertStringToFuzzyUUID(c);
        UUID zUUID = Utils.convertStringToFuzzyUUID(z);
        logger.info(a + ": " + aUUID.toString() + " " + aUUID.getMostSignificantBits());
        logger.info(b + ": " + bUUID.toString() + " " + bUUID.getMostSignificantBits());
        logger.info(c + ": " + cUUID.toString() + " " + cUUID.getMostSignificantBits());
        logger.info(z + ": " + zUUID.toString() + " " + zUUID.getMostSignificantBits());

        assertTrue(aUUID.getMostSignificantBits() < bUUID.getMostSignificantBits());
        assertTrue(bUUID.getMostSignificantBits() < cUUID.getMostSignificantBits());
        assertTrue(cUUID.getMostSignificantBits() < zUUID.getMostSignificantBits());
    }

    /**
     * Test of convertStringToUUID method, of class Utils. Tests special chars.
     */
    @Test
    public void testConvertStringToUUIDSpecialChars()
    {
        System.out.println("convertStringToUUIDSpecialChars");
        String s = "~!#$%^&*()_+";
        UUID result = Utils.convertStringToFuzzyUUID(s);
        UUID result2 = Utils.convertStringToFuzzyUUID(s);
        assertEquals(result2, result);
        assertEquals("7e212324-255e-262a-0000-000000000000", result.toString());
        s = "このスライドページに移動";// i have no idea what language this is (or what it says); just stole it from the usergrid presentation (http://sssslide.com/speakerdeck.com/sungjuly/apache-usergrid-internal#49)
        result = Utils.convertStringToFuzzyUUID(s);
        result2 = Utils.convertStringToFuzzyUUID(s);
        assertEquals(result2, result);
        assertEquals("e38193e3-81ae-e382-0000-000000000000", result.toString());
    }


    @Test
    public void testSetField() throws Exception
    {
        //negative tests only for now
        System.out.println("setField");
        DBObject object = new BasicDBObject();
        BoundStatement bs = null;
        IndexField fieldData = new IndexField("testField");
        //(DBObject jsonObject, IndexField fieldData, BoundStatement bs, int index)
        boolean normal = Utils.setField(object, fieldData, bs, 0);
        assertFalse("Expected exception not thrown.", normal);

        fieldData = new IndexField("testField", FieldDataType.INTEGER);
        object.put("testField", "thisisnotaninteger");
        boolean expectedExceptionThrown = false;
        try
        {
            Utils.setField(object, fieldData, bs, 0);
        } catch (IndexParseException e)
        {
            expectedExceptionThrown = true;
            assertTrue(e.getMessage().contains("thisisnotaninteger"));
            assertTrue(e.getMessage().contains("testField"));
        }
        assertTrue("Expected exception not thrown.", expectedExceptionThrown);

    }

    /**
     * Test of join method, of class Utils.
     */
    @Test
    public void testJoin_String_ObjectArr()
    {
        System.out.println("join");
        String expResult = "one";
        String result = Utils.join(", ", "one");
        assertEquals(expResult, result);
        expResult = "one, two";
        result = Utils.join(", ", "one", "two");
        assertEquals(expResult, result);
    }

    /**
     * Test of join method, of class Utils.
     */
    @Test
    public void testJoin_String_Collection()
    {
        System.out.println("join");
        List<String> list = new ArrayList<>();
        String expResult = "";
        String result = Utils.join(", ", list);
        assertEquals(expResult, result);
        list.add("one");
        expResult = "one";
        result = Utils.join(", ", list);
        assertEquals(expResult, result);
        list.add("two");
        expResult = "one, two";
        result = Utils.join(", ", list);
        assertEquals(expResult, result);
    }

    /**
     * Test of equalLists method, of class Utils.
     */
    @Test
    public void testEqualLists()
    {
        System.out.println("equalLists");
        List<String> one = null;
        List<String> two = null;
        boolean result = Utils.equalLists(one, two);
        assertEquals(true, result);
        
        one = new ArrayList<>();
        two = new ArrayList<>();
        result = Utils.equalLists(one, two);
        assertEquals(true, result);
                
        one.add("test");
        two.add("test");
        result = Utils.equalLists(one, two);
        assertEquals(true, result);
        
        two.add("testtest");
        result = Utils.equalLists(one, two);
        assertEquals(false, result);
        
        two = new ArrayList<>();
        result = Utils.equalLists(one, two);
        assertEquals(false, result);
    }
}
