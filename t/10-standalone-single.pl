use strict;
use warnings;

use lib qw(t);

use DBI;
use InclineTest;
use Test::More;

my $db = init_db(
    mysqld => {
        my_cnf => {
            'bind-address'           => '127.0.0.1',
            port                     => 19010,
            'default-storage-engine' => 'INNODB',
        },
    },
    postgresql => {
        port => 19010,
    },
);

my $dbh = DBI->connect($db->dsn)
    or die $DBI::errstr;

# create tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_dest incline_src/;
ok(
    $dbh->do('CREATE TABLE incline_dest (_id INT NOT NULL,_message VARCHAR(255) NOT NULL,PRIMARY KEY(_id))'),
    'create dest table',
);
ok(
    $dbh->do(
        adjust_ddl(
            'CREATE TABLE incline_src (id SERIAL,message VARCHAR(255) NOT NULL,PRIMARY KEY (id))',
        ),
    ),
    'create dest table',
);

# preload some data
$dbh->do(q{INSERT INTO incline_src (message) VALUES ('hola'),('bonjour')});

# load rules
system(
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--source=example/single.json --port=19010 --database=test
       create-trigger),
) == 0 or die "src/incline failed: $?";

# run tests
my $cmpf = sub {
    return (
        $dbh->selectall_arrayref('SELECT * FROM incline_src'),
        $dbh->selectall_arrayref('SELECT * FROM incline_dest'),
    );
};
is(
    $dbh->selectrow_arrayref('SELECT COUNT(*) FROM incline_dest')->[0],
    0,
    'no data',
);
$dbh->do('UPDATE incline_src SET id=id');
is_deeply($cmpf->(), 'post synch check');
ok(
    $dbh->do(q{INSERT INTO incline_src (message) VALUES ('hello')}),
    'insert',
);
is_deeply($cmpf->(), 'post insertion check');
ok(
    $dbh->do(q{INSERT INTO incline_src (message) VALUES ('hello'),('ciao')}),
    'insert',
);
is_deeply($cmpf->(), 'post insertion check');
ok(
    $dbh->do(q{UPDATE incline_src SET message='good bye' WHERE id%2!=0}),
    'update',
);
is_deeply($cmpf->(), 'post update check');
ok(
    $dbh->do('DELETE FROM incline_src WHERE id%2=0'),
    'delete',
);
is_deeply($cmpf->(), 'post delete check');

# drop tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_dest incline_src/;

done_testing;
