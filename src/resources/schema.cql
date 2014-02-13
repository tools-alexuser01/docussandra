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
	name text,
	primary key ((tid), id)
);

drop table if exists namespaces_name;
create table namespaces_name (
	id uuid,	// Namespace ID
	tid uuid,	// Tenant ID
	name text,
	primary key ((tid), name)
);

drop table if exists collections;
create table collections (
	id uuid,	// Collection ID (cid)
	nid uuid,	// Namespace ID
	tid uuid,	// Tenant ID
	name text,
	description text,
	primary key ((tid, nid), id)
);

drop table if exists collections_name;
create table collections_name (
	id uuid,	// Collection ID (cid)
	nid uuid,	// Namespace ID
	tid uuid,	// Tenant ID
	name text,
	description text,
	primary key ((tid, nid), name)
);

drop table if exists entities;
create table entities (
	id uuid,	// Entity ID (eid)
	nid uuid,	// Namespace ID
	cid uuid,	// Collection ID
	tid uuid,	// Tenant ID
	column blob,	// Column name
	value blob,	// Column value
	primary key ((tid, id), column)
);

drop table if exists indexes;
create table indexes (
	id uuid,	// Index ID (iid)
	nid uuid,	// Namespace ID
	cid uuid,	// Collection ID
	tid uuid,	// Tenant ID
	name text,	// Index name
	key text,	// Column name to index (key)
	primary key ((tid, nid), cid, id)
);

drop table if exists indexes_name;
create table indexes_name (
	id uuid,	// Index ID (iid)
	nid uuid,	// Namespace ID
	cid uuid,	// Collection ID
	tid uuid,	// Tenant ID
	name text,	// Index name
	key text,	// Column name to index (key)
	is_ascending boolean,
	primary key ((tid, nid), cid, name)
);

drop table if exists entities_idx;
create table entities_idx (
	id uuid,	// Entity ID
	nid uuid,	// Namespace ID (not really needed?)
	cid uuid,	// Collection ID
	tid uuid,	// Tenant ID
	iid uuid,	// Index ID
	key blob,	// Key value (indexed value)
	value blob,	// Entity blob (BSON)
	primary key (tid, nid, cid, iid, key)
);

begin batch
// Pearson eCollege
insert into tenants (id, name) values (89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 'pearson-ecollege');

insert into namespaces (id, tid, name) values(57493f29-4a81-4c18-8952-4cc9e088a296, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 'blogging-api');
insert into namespaces_name (id, tid, name) values(57493f29-4a81-4c18-8952-4cc9e088a296, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 'blogging-api');
insert into namespaces (id, tid, name) values(fa3fa86c-e71a-46e1-8631-5f636de26348, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 'remarks-api');
insert into namespaces_name (id, tid, name) values(fa3fa86c-e71a-46e1-8631-5f636de26348, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 'remarks-api');

insert into collections (id, tid, nid, name) values(3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 57493f29-4a81-4c18-8952-4cc9e088a296, 'blogs');
insert into collections_name (id, tid, nid, name) values(3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 57493f29-4a81-4c18-8952-4cc9e088a296, 'blogs');

insert into collections (id, tid, nid, name) values(15c69ab4-298d-40b1-9b7d-3d3a998ff8da, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 57493f29-4a81-4c18-8952-4cc9e088a296, 'posts');
insert into collections_name (id, tid, nid, name) values(15c69ab4-298d-40b1-9b7d-3d3a998ff8da, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 57493f29-4a81-4c18-8952-4cc9e088a296, 'posts');

insert into collections (id, tid, nid, name) values(7aa85c0d-3371-4c86-b87a-5f651c08cff2, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 57493f29-4a81-4c18-8952-4cc9e088a296, 'comments');
insert into collections_name (id, tid, nid, name) values(7aa85c0d-3371-4c86-b87a-5f651c08cff2, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, 57493f29-4a81-4c18-8952-4cc9e088a296, 'comments');

insert into collections (id, tid, nid, name) values(2c714213-bdf6-426f-9ea6-103a15aae0c4, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, fa3fa86c-e71a-46e1-8631-5f636de26348, 'remarks');
insert into collections_name (id, tid, nid, name) values(2c714213-bdf6-426f-9ea6-103a15aae0c4, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, fa3fa86c-e71a-46e1-8631-5f636de26348, 'remarks');

insert into collections (id, tid, nid, name) values(066cf525-4258-4291-a227-4d26ec7d35c0, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, fa3fa86c-e71a-46e1-8631-5f636de26348, 'replies');
insert into collections_name (id, tid, nid, name) values(066cf525-4258-4291-a227-4d26ec7d35c0, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, fa3fa86c-e71a-46e1-8631-5f636de26348, 'replies');

insert into entities (id, nid, cid, tid, column, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 57493f29-4a81-4c18-8952-4cc9e088a296, 3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, textAsBlob('name'), textAsBlob('Some old blog'));
insert into entities (id, nid, cid, tid, column, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 57493f29-4a81-4c18-8952-4cc9e088a296, 3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, textAsBlob('description'), textAsBlob('This is a blog about nothing... much like a Jerry Seinfeld sorta thing.'));
insert into entities (id, nid, cid, tid, column, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 57493f29-4a81-4c18-8952-4cc9e088a296, 3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, textAsBlob('createdAt'), textAsBlob('2012-12-13'));
insert into entities (id, nid, cid, tid, column, value) values (b52eb45b-6f1a-470f-a27a-14161a624ea4, 57493f29-4a81-4c18-8952-4cc9e088a296, 3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, textAsBlob('updatedAt'), textAsBlob('2012-12-13'));

insert into entities (id, nid, cid, tid, column, value) values (cddbd454-fae2-4660-8700-8570d05af24c, 57493f29-4a81-4c18-8952-4cc9e088a296, 3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, textAsBlob('name'), textAsBlob('Another Blog'));
insert into entities (id, nid, cid, tid, column, value) values (cddbd454-fae2-4660-8700-8570d05af24c, 57493f29-4a81-4c18-8952-4cc9e088a296, 3dc40a46-34b1-47fc-9981-6501f29bdcc3, 89ea7f73-16dc-42e6-b8db-3c3bea87bc0e, textAsBlob('description'), textAsBlob('There is just nothing here.');

// Strategic Gains
insert into tenants (id, name) values (4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, 'strategic-gains');

insert into namespaces (id, tid, name) values(fc37e764-29d4-4a54-ba36-d1363aee2725, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, 'core-api');
insert into namespaces_name (id, tid, name) values(fc37e764-29d4-4a54-ba36-d1363aee2725, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, 'core-api');

insert into collections (id, tid, nid, name) values(78b50520-99b7-42fa-9361-abccc1bf8378, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'users');
insert into collections_name (id, tid, nid, name) values(78b50520-99b7-42fa-9361-abccc1bf8378, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'users');

insert into collections (id, tid, nid, name) values(9f847466-79a6-41d2-b892-04ad6c7d97fe, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'applications');
insert into collections_name (id, tid, nid, name) values(9f847466-79a6-41d2-b892-04ad6c7d97fe, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'applications');

insert into collections (id, tid, nid, name) values(e99dfcf8-cc5b-4f26-a748-f172b67afc81, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'tokens');
insert into collections_name (id, tid, nid, name) values(e99dfcf8-cc5b-4f26-a748-f172b67afc81, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'tokens');

insert into collections (id, tid, nid, name) values(06ea358f-d5f0-48fe-ba45-e2755902ff35, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'organizations');
insert into collections_name (id, tid, nid, name) values(06ea358f-d5f0-48fe-ba45-e2755902ff35, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'organizations');

insert into collections (id, tid, nid, name) values(d23edd26-e948-41ef-91e7-3af61545c899, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'roles');
insert into collections_name (id, tid, nid, name) values(d23edd26-e948-41ef-91e7-3af61545c899, 4d16b840-6fc3-4fc5-a5b7-b11507f8b7ad, fc37e764-29d4-4a54-ba36-d1363aee2725, 'roles');

apply batch;