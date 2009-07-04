use strict;
use warnings;

use DBI;
use Test::More tests => 14;

my $dbh = DBI->connect('dbi:mysql:test;user=root')
    or die DBI->errstr;

# create tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_dest incline_src/;
ok(
    $dbh->do('CREATE TABLE incline_dest (_id INT UNSIGNED NOT NULL,_message VARCHAR(255) NOT NULL,PRIMARY KEY(_id)) ENGINE=InnoDB'),
    'create dest table',
);
ok(
    $dbh->do('CREATE TABLE incline_src (id INT UNSIGNED NOT NULL AUTO_INCREMENT,message VARCHAR(255) NOT NULL,PRIMARY KEY (id)) ENGINE=InnoDB'),
    'create dest table',
);

# load rules
system(qw(src/incline --source=example/single.json --database=test create-trigger)) == 0
    or die "src/incline failed: $?";

# run tests
my $cmpf = sub {
    return (
        $dbh->selectall_arrayref('SELECT * FROM incline_src'),
        $dbh->selectall_arrayref('SELECT * FROM incline_dest'),
    );
};
ok(
    $dbh->do('INSERT INTO incline_src (message) VALUES ("hello")'),
    'insert',
);
is_deeply($cmpf->(), 'post insertion check');
ok(
    $dbh->do('INSERT INTO incline_src (message) VALUES ("hello"),("ciao")'),
    'insert',
);
is_deeply($cmpf->(), 'post insertion check');
ok(
    $dbh->do('UPDATE incline_src SET message="good bye" WHERE id%2!=0'),
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
