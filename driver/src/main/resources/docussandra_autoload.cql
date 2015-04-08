create keyspace IF NOT EXISTS docussandra
with replication = { 'class' : 'NetworkTopologyStrategy', 'use1' : 2, 'usw2' : 2};
use docussandra;

create table IF NOT EXISTS sys_meta (
	id text,
	version text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((id), updated_at)
)
with clustering order by (updated_at DESC);

create table IF NOT EXISTS sys_db (
	db_name text primary key,
	description text,
	created_at timestamp,
	updated_at timestamp
);

create table IF NOT EXISTS sys_tbl (
	db_name text,
	tbl_name text,
	description text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name)
);

create table IF NOT EXISTS sys_idx (
	db_name text,
	tbl_name text,
	name text,
	is_unique boolean,
	bucket_sz bigint,
	fields list<text>,
	only list<text>,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name, name)
);
