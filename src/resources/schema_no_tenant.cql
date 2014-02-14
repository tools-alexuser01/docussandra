drop keyspace if exists mongossandra;
create keyspace mongossandra
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use mongossandra;

drop table if exists namespaces;
create table namespaces (
	id uuid primary key,
	name text,
	created_at timestamp,
	updated_at timestamp
);

drop table if exists namespaces_name;
create table namespaces_name (
	id uuid,
	name text primary key,
	created_at timestamp,
	updated_at timestamp
);

drop table if exists collections;
create table collections (
	id uuid,
	namespace_id uuid,
	name text,
	description text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((namespace_id), id)
);

drop table if exists collections_name;
create table collections_name (
	id uuid,
	namespace_id uuid,
	name text,
	description text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((namespace_id), name)
);

drop table if exists entities;
create table entities (
	id uuid primary key,
	namespace_id uuid,
	collection_id uuid,
	object blob
);

// To support cascade-delete
create index on entities (namespace_id);
create index on entities (collection_id);

drop table if exists entities_idx;
create table entities_idx (
	id uuid,
	namespace_id uuid,
	collection_id uuid,
	key blob,	// Indexed column name(s)
	value blob,	// Indexed value
	object blob,
	primary key ((namespace_id, collection_id, key, value), id)
//	primary key ((namespace_id, collection_id, key), value, id)
);
