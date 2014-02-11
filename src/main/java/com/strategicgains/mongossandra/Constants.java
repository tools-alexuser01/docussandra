package com.strategicgains.mongossandra;

public class Constants
{
	/**
	 * These define the URL parmaeters used in the route definition strings (e.g. '{userId}').
	 */
	public class Url
	{
		//TODO: Your URL parameter names here...
		public static final String UUID = "uuid";
		public static final String KEY1 = "key1";
		public static final String KEY2 = "key2";
		public static final String KEY3 = "key3";
		public static final String DATABASE_ID = "databaseId";
		public static final String TABLE_ID = "tableId";
		public static final String ENTITY_ID = "entityId";
		public static final String INDEX_ID = "indexId";
		public static final String QUERY_ID = "queryId";
	}

	/**
	 * These define the route names used in naming each route definitions.  These names are used
	 * to retrieve URL patterns within the controllers by name to create links in responses.
	 */
	public class Routes
	{
		//TODO: Your Route names here...
		public static final String SINGLE_UUID_SAMPLE = "sample.uuid.single.route";
		public static final String SINGLE_COMPOUND_SAMPLE = "sample.compound.single.route";
		public static final String SAMPLE_UUID_COLLECTION = "sample.uuid.collection.route";
		public static final String SAMPLE_COMPOUND_COLLECTION = "sample.compound.collection.route";
		public static final String DATABASES = "database.collection.route";
		public static final String DATABASE = "database.single.route";
		public static final String TABLES = "table.collection.route";
		public static final String TABLE = "table.single.route";
		public static final String ENTITIES = "entity.collection.route";
		public static final String ENTITY = "entity.single.route";
		public static final String INDEXES = "index.collection.route";
		public static final String INDEX = "index.single.route";
		public static final String QUERIES = "query.collection.route";
		public static final String QUERY = "query.single.route";
	}
}
