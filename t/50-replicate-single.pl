use strict;
use warnings;

use lib qw(t);

use DBI;
use InclineTest;
use Test::More;

# skip tests if dbms does not exist
init_db(
    mysqld => {
        my_cnf => { 'skip-networking' => '' },
    },
    postgresql => {},
);

plan tests => 11;

my @incline_cmd = (
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--mode=shard --source=example/replicate-single.json),
    qw(--database=test),
);
my @db_nodes = (
    qw/127.0.0.1:19010 127.0.0.1:19011 127.0.0.1:19012/, # use the first three
);
my @db;
my @dbh;

diag("please ignore diag messages unless any test fails");

for my $db_node (@db_nodes) {
    my ($db_host, $db_port) = split /:/, $db_node, 2;
    # start db
    push @db, init_db(
        mysqld => {
            my_cnf => {
                'bind-address'           => $db_host,
                port                     => $db_port,
                'default-storage-engine' => 'INNODB',
            },
        },
        postgresql => {
            port => $db_port,
        },
    );
    # create tables
    push @dbh, do {
        my $dbh = DBI->connect($db[-1]->dsn)
            or die $DBI::errstr;
        $dbh;
    };
    ok($dbh[-1]->do("DROP TABLE IF EXISTS $_"), "drop $_")
        for qw/incline_src incline_dest/;
    if (@dbh == 1) {
        ok(
            $dbh[-1]->do(
                adjust_ddl('CREATE TABLE incline_src (id SERIAL,message VARCHAR(255) NOT NULL,PRIMARY KEY (id))'),
            ),
            'create source table',
        );
        ok(
            system(
                @incline_cmd,
                "--host=$db_host",
                "--port=$db_port",
                'create-queue',
            ) == 0,
            'create queue',
        );
        ok(
            system(
                @incline_cmd,
                "--host=$db_host",
                "--port=$db_port",
                'create-trigger',
            ) == 0,
            'create trigger',
        );
    } else {
        ok(
            $dbh[-1]->do(
                'CREATE TABLE incline_dest (_id INT NOT NULL,_message VARCHAR(255) NOT NULL,PRIMARY KEY (_id))',
            ),
            'create dest table',
        );
    }
}

1;
