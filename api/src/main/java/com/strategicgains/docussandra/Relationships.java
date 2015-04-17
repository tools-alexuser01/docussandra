/*
    Copyright 2014, Strategic Gains, Inc.

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
package com.strategicgains.docussandra;

import static com.strategicgains.docussandra.Constants.Routes.DATABASE;
import static com.strategicgains.docussandra.Constants.Routes.DATABASES;
import static com.strategicgains.docussandra.Constants.Routes.INDEX;
import static com.strategicgains.docussandra.Constants.Routes.TABLE;
import static com.strategicgains.docussandra.Constants.Routes.TABLES;
import static com.strategicgains.docussandra.Constants.Routes.INDEXES;
import static com.strategicgains.docussandra.Constants.Routes.INDEX_STATUS;
import static com.strategicgains.hyperexpress.RelTypes.SELF;
import static com.strategicgains.hyperexpress.RelTypes.UP;

import java.util.Map;

import org.restexpress.RestExpress;

import com.strategicgains.docussandra.domain.Database;
import com.strategicgains.docussandra.domain.Index;
import com.strategicgains.docussandra.domain.IndexCreationStatus;
import com.strategicgains.docussandra.domain.Table;
import com.strategicgains.hyperexpress.HyperExpress;

/**
 * Warning: Do not code format this file, it will make it harder to read.
 * @author toddf
 * @since Jun 12, 2014
 */
public class Relationships
{
	public static void define(RestExpress server)
	{
		Map<String, String> routes = server.getRouteUrlsByName();

		HyperExpress.relationships()
		.forCollectionOf(Database.class)
			.rel(SELF, routes.get(DATABASES))

		.forClass(Database.class)
			.rel(SELF, routes.get(DATABASE))
			.rel(UP, routes.get(DATABASES))
			.rel("collections", routes.get(TABLES))
				.title("The collections in this namespace")

		.forCollectionOf(Table.class)
			.rel(SELF, routes.get(TABLES))
			.rel(UP, routes.get(DATABASE))
				.title("The namespace containing this collection")

		.forClass(Table.class)
			.rel(SELF, routes.get(TABLE))
			.rel(UP, routes.get(TABLES))
				.title("The entire list of collections in this namespace")

                .forCollectionOf(Index.class)
			.rel(SELF, routes.get(INDEXES))
			.rel(UP, routes.get(TABLE))
			.title("The collection that this index was created on.")
        
                .forClass(Index.class)
			.rel(SELF, routes.get(INDEX))
			.rel(UP, routes.get(INDEXES))
				.title("The list of indexes for this collection.")
                
                   //N/A -- this is a global status of all current indexing operations
//                .forCollectionOf(IndexCreationStatus.class)
//			.rel(SELF, routes.get(INDEX))
//			.rel(UP, routes.get(INDEXES))
                        
                .forClass(IndexCreationStatus.class)
			.rel(SELF, routes.get(INDEX_STATUS))
			.rel(UP, routes.get(INDEX))
                        .rel("index", routes.get(INDEX))
				.title("The index for this status.");
                
//			.rel("documents", routes.get(DOCUMENTS))
//				.title("The documents in this collection")
//
//		.forCollectionOf(Document.class)
//			.rel(SELF, routes.get(DOCUMENTS))
//			.rel(UP, routes.get(TABLE))
//				.title("The collection containing this document")
//
//		.forClass(Document.class)
//			.rel(SELF, routes.get(DOCUMENT))
//			.rel(UP, routes.get(DOCUMENTS))
//				.title("The entire list of documents in this collection");
	}
}
