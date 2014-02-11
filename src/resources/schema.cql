drop keyspace if exists mongossandra;
create keyspace mongossandra
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use mongossandra;

drop table if exists tenants;
create table tenants (
	id uuid, // Tenant ID (tid)
	name text,
	primary key (id)
);

drop table if exists namespaces;
create table namespaces (
	id uuid, 	// Namespace ID (nid)
	tid uuid, 	// Tenant ID
	namespace text,
	primary key (id, tid)
);

drop table if exists namespaces_name;
create table namespaces_name (
	id uuid,	// Namespace ID
	tid uuid,	// Tenant ID
	namespace text,
	primary key (namespace, tid)
);

drop table if exists collections;
create table collections (
	id uuid,	// Collection ID (cid)
	nid uuid,	// Namespace ID
	tid uuid,	// Tenant ID
	name text,
	description text,
	primary key (id, tid, nid)
);

drop table if exists collections_name;
create table collections_name (
	id uuid,	// Collection ID (cid)
	nid uuid,	// Namespace ID
	tid uuid,	// Tenant ID
	name text,
	description text,
	primary key ((nid, tid), name)
);

drop table if exists entities;
create table entities_id (
	id uuid,	// Entity ID (eid)
	nid uuid,	// Namespace ID
	cid uuid,	// Collection ID
	tid uuid,	// Tenant ID
	name blob,	// Column name
	value blob,	// Column value
	primary key ((id, tid), name)
);

drop table if exists indexes;
create table indexes (
	id uuid,	// Index ID (iid)
	nid uuid,	// Namespace ID
	cid uuid,	// Collection ID
	tid uuid,	// Tenant ID
	name text,	// Index name
	key text,	// Column name to index (key)
	primary key (cid, tid, id)
)

drop table if exists entities_idx;
create table entities_idx (
	id uuid,	// Entity ID
	nid uuid,	// Namespace ID
	cid uuid,	// Collection ID
	tid uuid,	// Tenant ID
	key blob,	// Key value (indexed value)
	value blob,	// Entity blob (BSON)
	primary key (cid, tid, key)
);
