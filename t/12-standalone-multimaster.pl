use strict;
use warnings;

use lib qw(t);

use DBI;
use InclineTest;
use Test::More;

my $instance = InclineTest->create_any(
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

plan tests => 23;

my $dbh = InclineTest->connect(
    'DBI:any(PrintWarn=>0):dbname=test;user=root;host=127.0.0.1;port=19010'
) or die DBI->errstr;

# create tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_tweet incline_follow incline_timeline/;
ok(
    $dbh->do(
        InclineTest->adjust_ddl(
            'CREATE TABLE incline_tweet (id SERIAL,user_id INT NOT NULL,body VARCHAR(255) NOT NULL,PRIMARY KEY(id))',
        ),
    ),
    'create tweet table',
);
ok(
    $dbh->do('CREATE TABLE incline_follow (followee INT NOT NULL,follower INT NOT NULL,PRIMARY KEY(followee,follower))'),
    'create follow table',
);
ok(
    $dbh->do('CREATE TABLE incline_timeline (user_id INT NOT NULL,tweet_id INT NOT NULL,PRIMARY KEY(user_id,tweet_id))'),
    'create timeline table',
);

# load rules
system(
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--source=example/multimaster.json --port=19010 --database=test),
    qw(create-trigger),
) == 0 or die "src/incline failed: $?";

# run tests
my $cmpf = sub {
    return (
        $dbh->selectall_arrayref('SELECT follow.follower,tweet.id,tweet.user_id,tweet.body FROM incline_tweet AS tweet INNER JOIN incline_follow AS follow ON follow.followee=tweet.user_id ORDER BY follow.follower,tweet.id'),
        $dbh->selectall_arrayref('SELECT timeline.user_id,tweet.id,tweet.user_id,tweet.body FROM incline_tweet AS tweet INNER JOIN incline_timeline AS timeline ON timeline.tweet_id=tweet.id ORDER BY timeline.user_id,tweet.id'),
    );
};
ok(
    $dbh->do('INSERT INTO incline_follow (followee,follower) VALUES (1,2),(2,1),(1,3),(1,4)'),
    'setup relations',
);
is_deeply($cmpf->(), 'post relations setup check');
ok(
    $dbh->do(q{INSERT INTO incline_tweet (user_id,body) VALUES (1,'hello')}),
    'tweet',
);
is_deeply($cmpf->(), 'post tweet check');
ok(
    $dbh->do(q{INSERT INTO incline_tweet (user_id,body) VALUES (2,'ciao')}),
    'tweet 2',
);
is_deeply($cmpf->(), 'post tweet check 2');
ok(
    $dbh->do(q{INSERT INTO incline_tweet (user_id,body) VALUES (3,'hola')}),
    'tweet 3',
);
is_deeply($cmpf->(), 'post tweet check 3');
ok(
    $dbh->do('INSERT INTO incline_follow (followee,follower) VALUES (2,4),(1,5)'),
    'add relation',
);
is_deeply($cmpf->(), 'post relation addition check');
ok(
    $dbh->do('DELETE FROM incline_tweet WHERE user_id=1'),
    'delete one tweet',
);
is_deeply($cmpf->(), 'post tweet deletion check');
ok(
    $dbh->do('DELETE FROM incline_follow WHERE followee=2 AND follower=1'),
    'delete relation',
);
is_deeply($cmpf->(), 'post relation deletion check');

# drop tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_cal incline_cal_member incline_cal_by_user/;

1;
