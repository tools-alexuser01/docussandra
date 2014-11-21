drop keyspace if exists docussandra;
create keyspace docussandra
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use docussandra;

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

drop table if exists indexes;
create table indexes (
	namespace text,
	collection text,
	name text,
	is_unique boolean,
	bucket_size bigint,
	fields list<text>,
	created_at timestamp,
	updated_at timestamp,
	primary key ((namespace, collection), name)
);

 drop table if exists doc_idx;
 create table doc_idx (
 	namespace text,
 	collection text,
	name text,		// Index name
	key blob,		// Indexed column name(s)
	id uuid,		// Entity ID
	object blob,	// Entity in BSON
	primary key ((namespace, collection, name), key, id)
);
