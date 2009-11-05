use strict;
use warnings;

use lib qw(t);

use DBI;
use InclineTest;
use Scope::Guard;
use Test::More;

# skip tests if dbms does not exist
init_db(
    mysqld => {
        my_cnf => { 'skip-networking' => '' },
    },
    postgresql => {},
);

plan tests => 27 + ($ENV{TEST_DBMS} ne 'postgresql' && 3);

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
    } else {
        ok(
            $dbh[-1]->do(
                'CREATE TABLE incline_dest (_id INT NOT NULL,_message VARCHAR(255) NOT NULL,PRIMARY KEY (_id))',
            ),
            'create dest table',
        );
    }
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
}

sub cmpf {
    my $slave = shift;
    my $n = $dbh[0]->selectall_arrayref(
        'SELECT MAX(_iq_id) FROM _iq_incline_dest',
    )->[0]->[0];
    sleep 1 while do {
        my $r = $dbh[$slave]->selectall_arrayref(
            q{SELECT last_id FROM _iq_repl WHERE tbl_name='incline_dest'},
        );
        $n != (@$r ? $r->[0]->[0] : 0);
    };
    return (
        $dbh[0]->selectall_arrayref('SELECT * FROM incline_src'),
        $dbh[$slave]->selectall_arrayref('SELECT * FROM incline_dest'),
    );
}

sub start_fw {
    my $fw_pid;
    unless ($fw_pid = fork()) {
        my ($db_host, $db_port) = split /:/, $db_nodes[0], 2;
        exec_cmd(
            @incline_cmd,
            "--host=$db_host",
            "--port=$db_port",
            'forward',
        );
        die "failed to exec forwarder: $?";
    }
    Scope::Guard->new(sub { kill 'TERM', $fw_pid if $fw_pid });
}

# start forwarder
my $fw = start_fw();

ok(
    $dbh[0]->do(q{INSERT INTO incline_src (message) VALUES ('hello')}),
    'insert',
);
is_deeply(cmpf($_), "post insertion check (node $_)")
    for qw/1 2/;
ok(
    $dbh[0]->do(q{UPDATE incline_src SET message='ciao'}),
    'update',
);
is_deeply(cmpf($_), "post update check (node $_)")
    for qw/1 2/;
ok(
    $dbh[0]->do(q{INSERT INTO incline_src (message) VALUES ('aloha')}),
    'insert 2',
);
is_deeply(cmpf($_), "post insertion check 2 (node $_)")
    for qw/1 2/;
ok(
    $dbh[0]->do(q{DELETE FROM incline_src WHERE message='ciao'}),
    'delete',
);
is_deeply(cmpf($_), "post deletion check (node $_)")
    for qw/1 2/;

# partially down test (TODO: support postmaster, that does not exit until all connections (including that from forwarder) closes)
if ($ENV{TEST_DBMS} ne 'postgresql') {
    undef $dbh[1];
    $db[1]->stop();
    ok(
        $dbh[0]->do(q{INSERT INTO incline_src (message) VALUES ('nihao')}),
        'partially down: insert',
    );
    is_deeply(cmpf(2), 'partially down: insert to nodes alive');
    $db[1]->start();
    $dbh[1] = DBI->connect($db[1]->dsn)
        or die $DBI::errstr;
    is_deeply(cmpf(1), 'partially down: reconnect and sync on recovery');
}

undef $fw;
@dbh = ();

1;
