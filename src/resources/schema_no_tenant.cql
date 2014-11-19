drop keyspace if exists mongossandra;
create keyspace mongossandra
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use mongossandra;

drop table if exists namespaces;
create table namespaces (
	name text primary key,
	description text,
	created_at timestamp,
	updated_at timestamp
);

drop table if exists collections;
create table collections (
	namespace text,
	name text,
	description text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((namespace), name)
);

drop table if exists documents;
create table documents (
	id uuid,
	namespace text,
	collection text,
	object blob,
	created_at timestamp,
	updated_at timestamp,
	primary key (id) //(namespace, collection, id)
);

create index on documents (namespace);
create index on documents (collection);

drop table if exists doc_idx;
create table doc_idx (
	namespace text,
	collection text,
	name text,
	is_unique boolean,
	bucket_size bigint,
	properties map<text, text>,
	created_at timestamp,
	updated_at timestamp,
	primary key ((namespace, collection), name)
);

//==================================
// From Data Extension Framework:
//----------------------------------
CREATE TABLE data_entity (
	id uuid PRIMARY KEY,
	type text,
	object text
);

CREATE INDEX ON data_entity (type);

CREATE TABLE data_entity_attribute (
	entity_type text,
	attribute_name text,
	attribute_value text,
	entity_id uuid,
	data_entity_object text,
	PRIMARY KEY ((entity_type, attribute_name, attribute_value), entity_id)
);
//==================================
