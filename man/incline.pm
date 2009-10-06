=head1 NAME

incline - a replicator for RDB shards

=head1 SYNOPSIS

incline [options] command

=head1 DESCRIPTION

Incline is a replicator for MySQL / PostgreSQL with following characteristics.

=over 4

=item * replicates information within a single database node or between database shards

=item * replication rules defined in JSON files

=item * synchronous replication within a single database through the use of automatically-generated triggers

=item * asynchronous (eventually consistent) replication between database nodes using automatically-generated queue tables and fault-torelant forwarders

=back

This manual consists of three parts, C<INSTALLATION>, C<TUTORIAL>, and C<COMMAND REFENENCE>.  For design documentation and background knowledge, please refer to the URLs listed in the C<SEE ALSO> section.

=head1 INSTALLATION

Incline uses C<autotools> and automatically tries to detect the client libraries of MySQL and / or PostgreSQL, so a typical installation procedure will be as follows.

    % ./configure
    % make
    # make install

If configure fails to locate the client libraries, --with-mysql and --with-pgsql options can be used.

    % ./configure --with-mysql=my_mysql_installation_dir

Also, if you have perl and its DBI drivers installed, it is possible to run the embedded tests using make.

    % make test

=head1 TUTORIAL

The tutorial explains how to create a microblog service (like twitter) running on four database shards.  Incline (by itself) does not support adding database nodes without stopping the service.  If you are interested in such feature, please refer to the documentation of C<Pacific> after reading this tutorial.

=head2 CREATING TABLES

At least four tables are needed to create a microblog service on database shards.  Instead of a single table representing follower E<lt>=E<gt> followee relationship, each user needs to have a list of followers (or list of following users) to him / her on his / her database shard.  Also, each user need to have his / her `timeline' table on his / her shard (or else the service would not scale out).  The example below is a minimal schema on MySQL.  All shards should have the same schema applied.  Two tables, `following' and `tweet' will be modified by the application.  `Follower' and `timeline' tables will be automatically kept (eventually) in sync by incline with the former two tables.

    CREATE TABLE following (
      userer_id INT UNSIGNED NOT NULL,
      following_id INT UNSIGNED NOT NULL,
      PRIMARY KEY (user_id,following_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    CREATE TABLE follower (
      user_id INT UNSIGNED NOT NULL,
      follower_id INT UNSIGNED NOT NULL,
      PRIMARY KEY (user,following_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    CREATE TABLE tweet (
      tweet_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
      user_id INT UNSIGNED NOT NULL,
      creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      body VARCHAR(255) NOT NULL,
      PRIMARY KEY (tweet_id),
      KEY (user_id,tweet_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    CREATE TABLE timeline (
      user_id INT UNSIGNED NOT NULL,
      tweet_user_id INT UNSIGNED NOT NULL,
      tweet_id INT UNSIGEND NOT NULL,
      creation_time TIMESTAMP NOT NULL,
      PRIMARY KEY (user_id,creation_time,tweet_user_id,tweet_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

=head2 WRITING THE REPLICATION DEFINITION FILE

To keep `follower' and `timeline' tables in sync with the other two, replication rules should be defined.  The example below show the definition corresponding to the table schema above.

The first hash defines how the `follower' tables should be kept synchronized to the `following' tables.  `Following_id' and `user_id' columns of `following' tables are mapped to `user_id' and `follower_id' columns of `follower' tables, and `follower' tables are sharded using the `user_id' column.

The second hash defines how the `timeline' tables should be constructed from the `follower' tables and `tweet' tables.  In addition to the definitions of `pk_columns' and `shard-key', `merge' property of the hash defines how the two source tables should be merged (using INNER JOIN).

    [
      {
        "destination" : "follower",
        "source"      : "following",
        "pk_columns"  : {
          "following.following_id" : "user_id",
          "following.user_id"      : "follower_id"
        },
        "shard-key"   : "user_id"
      },
      {
        "destination" : "timeline",
        "source"      : [ "follower", "tweet" ],
        "pk_columns"  : {
          "follower.follower_id" : "user_id",
          "tweet.user_id"        : "tweet_user_id",
          "tweet.tweet_id"       : "tweet_id",
          "tweet.creation_time"  : "creation_time"
        },
        "merge"       : {
          "follower.user_id" : "tweet.user_id"
        },
        "shard-key"   : "user_id"
      }
    ]

=head2 WRITING THE SHARD DEFINITION FILE

Another definition file is required when using incline for synchronizing database shards.  The following example represents a distributed database with four shards using range partitioning.  First node with the IP address 10.1.1.1 handles ids from 0 to 9999, second node (10.1.1.2) handles 10000 to 19999, third (10.1.1.1.3) handles 20000 to 29999, fourth (10.1.1.4) handles ids equal to or greater than 3000.

    {
      "algorithm" : "range-int",
      "map"       : {
        "0"     : [ {
          "host" : "10.1.1.1"
        } ],
        "10000" : [ {
          "host" : "10.1.1.2"
        } ],
        "20000" : [ {
          "host" : "10.1.1.3"
        } ],
        "30000" : [ {
          "host" : "10.1.1.4"
        } ]
      }
    }

In addition to `range-int', `hash-int' algorithm is also supported.  A hash-based shard definition will look like below, you may use either one to run the microblog service described in this tutorial.

    {
      "algorithm" : "hash-int",
      "num"       : 4,
      "nodes"     : [
        [ {
          "host" : "10.1.1.1"
        } ],
        [ {
          "host" : "10.1.1.2"
        } ],
        [ {
          "host" : "10.1.1.3"
        } ],
        [ {
          "host" : "10.1.1.4"
        } ]
      ]
    }

=head2 INSTALLING QUEUE TABLES AND TRIGGERS

The next step is to install triggers and to create queue tables using the definitions files.  The following commands create queue tables and installs triggers on the database running on 10.1.1.1.  The commands should be applied to all of the database shards.

    % incline --rdbms=mysql --database=microblog --host=10.1.1.1 \
     --user=root --password=XXXXXXXX --mode=shard \
     --source=replication.json --shard-source=shard.json create-queue
    % incline --rdbms=mysql --database=microblog --host=10.1.1.1 \
     --user=root --password=XXXXXXXX --mode=shard \
     --source=replication.json --shard-source=shard.json create-trigger

The files, `replication.json' and `shard.json' should contain the definitions shown in the sections above.

=head2 RUNNING THE FORWARDER

To transfer modifications between database shards, forwarders should be run attached to each shard.  The example below starts a forwarder process attached to 10.1.1.1.

    % incline --rdbms=mysql --database=microblog --host=10.1.1.1 \
     --user=root --password=XXXXXXXX --mode=shard \
     --source=replication.json --shard-source=shard.json forward

You should automatically restart the forwarder when it exits (it exits under certain conditions, for example, when it loses connection to the attached shard, or when the shard definition is being updated).

=head2 SETUP COMPLETE

Now the whole system is up and running.  You can try insert / update / delete the rows in `following' or `tweet' table and see the other tables updated by incline.

    # User:100 starts following user:10100.  `Follower' table on 10.1.1.2
    # (the shard for user:10100) will be updated
    10.1.1.1> INSERT INTO following (user_id,following_id) VALUES \
              (100,10100);
    10.1.1.2> SELECT * FROM follower WHEER user_id=10100;
    +---------+-------------+
    | user_id | follower_id |
    +---------+-------------+
    |   10100 |         100 |
    +---------+-------------+
    1 row in set (0.00 sec)

    # User:10100 tweets.  `Timeline' table on 10.1.1.1 will be updated.
    10.1.1.2> INSERT INTO tweet (user_id,body) VALUES (10100,'hello');
    10.1.1.1> SELECET * FROM timeline WHERE user_id=100;
    +---------+---------------+----------+---------------------+
    | user_id | tweet_user_id | tweet_id |    creation_time    |
    +---------+---------------+----------+---------------------+
    |     100 |         10100 |        1 | 2009-10-05 20:32:07 |
    +---------+---------------+----------+---------------------+
    1 row in set (0.00 sec)

=head1 COMMAND REFERENCE

reference comes here

=head1 SEE ALSO

Incline & Pacific (in Japanese)
http://www.slideshare.net/kazuho/incline-pacific

A Clever Way to Scale-out a Web Application
http://www.slideshare.net/kazuho/a-clever-way-to-scaleout-a-web-application

Kazuho@Cybozu Labs: Intruducing Incline - a synchronization tool for RDB shards (outdated)
http://developer.cybozu.co.jp/kazuho/2009/07/intruducing-inc.html

=head1 AUTHOR

Kazuho Oku E<lt>kazuhooku@gmail.comE<gt>

=head1 LICENSE

Copyright (c) 2009, Cybozu Labs, Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

=over 4

=item * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

=item * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

=item * Neither the name of Cybozu Labs, Inc. nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

=back

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut
