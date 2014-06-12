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
package com.strategicgains.mongossandra;

import static com.strategicgains.mongossandra.Constants.Routes.*;
import static com.strategicgains.hyperexpress.RelTypes.SELF;
import static com.strategicgains.hyperexpress.RelTypes.UP;

import java.util.Map;

import org.restexpress.RestExpress;

import com.strategicgains.hyperexpress.HyperExpress;
import com.strategicgains.mongossandra.domain.Collection;
import com.strategicgains.mongossandra.domain.Document;
import com.strategicgains.mongossandra.domain.Namespace;

/**
 * @author toddf
 * @since Jun 12, 2014
 */
public class Relationships
{
	public static void define(RestExpress server)
	{
		Map<String, String> routes = server.getRouteUrlsByName();

		HyperExpress.relationships()
		.addNamespace(new com.strategicgains.hyperexpress.domain.Namespace("collections", "http://namespaces.strategicgains.com/rel/{rel}"))
		.addNamespace(new com.strategicgains.hyperexpress.domain.Namespace("documents", "http://namespaces.strategicgains.com/rel/{rel}"))

		.forCollectionOf(Namespace.class)
			.rel(SELF, routes.get(NAMESPACES))

		.forClass(Namespace.class)
			.rel(SELF, routes.get(NAMESPACE))
			.rel(UP, routes.get(NAMESPACES))
			.rel("collections", routes.get(COLLECTIONS))
				.title("The collections in this namespace")

		.forCollectionOf(Collection.class)
			.rel(SELF, routes.get(COLLECTIONS))
			.rel(UP, routes.get(NAMESPACE))
				.title("The namespace containing this collection")

		.forClass(Collection.class)
			.rel(SELF, routes.get(COLLECTION))
			.rel(UP, routes.get(COLLECTIONS))
				.title("The entire list of collections in this namespace")
			.rel("documents", routes.get(DOCUMENTS))
				.title("The documents in this collection")

		.forCollectionOf(Document.class)
			.rel(SELF, routes.get(DOCUMENTS))
			.rel(UP, routes.get(COLLECTION))
				.title("The collection containing this document")

		.forClass(Document.class)
			.rel(SELF, routes.get(DOCUMENT))
			.rel(UP, routes.get(DOCUMENTS))
				.title("The entire list of documents in this collection");
	}
}
