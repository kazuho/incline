use strict;
use warnings;

use DBI;
use Test::More tests => 16;

my $dbh = DBI->connect('dbi:mysql:test;user=root')
    or die DBI->errstr;

# create tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_cal incline_cal_member incline_cal_by_user/;
ok(
    $dbh->do('CREATE TABLE incline_cal (id INT UNSIGNED NOT NULL,at INT UNSIGNED NOT NULL,title VARCHAR(255) NOT NULL,PRIMARY KEY(id),KEY at(at))'),
    'create cal table',
);
ok(
    $dbh->do('CREATE TABLE incline_cal_member (cal_id INT UNSIGNED NOT NULL,user_id INT UNSIGNED NOT NULL,PRIMARY KEY(cal_id,user_id))'),
    'create cal_member table',
);
ok(
    $dbh->do('CREATE TABLE incline_cal_by_user (_user_id INT UNSIGNED NOT NULL,_cal_id INT UNSIGNED NOT NULL,_at INT UNSIGNED NOT NULL,PRIMARY KEY(_user_id,_cal_id),KEY user_id_at_cal_id (_user_id,_at,_cal_id))'),
    'create cal_by_user table',
);

# load rules
system(qw(src/incline --source=example/standalone-singlemaster.json --database=test create-trigger)) == 0
    or die "src/incline failed: $?";

# run tests
my $cmpf = sub {
    return (
        $dbh->selectall_arrayref('SELECT user_id,cal_id,at FROM incline_cal INNER JOIN incline_cal_member ON incline_cal.id=incline_cal_member.cal_id ORDER BY user_id,cal_id'),
        $dbh->selectall_arrayref('SELECT _user_id,_cal_id,_at FROM incline_cal_by_user ORDER BY _user_id,_cal_id'),
    );
};
ok(
    $dbh->do('INSERT INTO incline_cal (id,at,title) VALUES (1,999,"hello")'),
    'insert into cal',
);
ok(
    $dbh->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (1,101),(1,102)'),
    'insert into cal_member',
);
is_deeply($cmpf->(), 'post insertion check');
ok(
    $dbh->do('UPDATE incline_cal SET at=at+1'),
    'update dependent table',
);
is_deeply($cmpf->(), 'post deletion from master check');
ok(
    $dbh->do('DELETE FROM incline_cal_member WHERE user_id=102'),
    'delete from master',
);
is_deeply($cmpf->(), 'post deletion from master check');

# drop tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_cal incline_cal_member incline_cal_by_user/;
