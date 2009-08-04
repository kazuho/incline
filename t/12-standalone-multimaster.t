use strict;
use warnings;

use DBI;
use Test::mysqld;

use Test::More tests => 23;

my $mysqld = Test::mysqld->new(
    my_cnf => {
        'bind-address' => '127.0.0.1',
        port           => 19010,
    },
);
my $dbh = DBI->connect(
    'dbi:mysql:test;user=root;mysql_socket=' . $mysqld->my_cnf->{socket},
) or die DBI->errstr;

# create tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_tweet incline_follow incline_timeline/;
ok(
    $dbh->do('CREATE TABLE incline_tweet (id INT UNSIGNED NOT NULL AUTO_INCREMENT,user_id INT UNSIGNED NOT NULL,body VARCHAR(255) NOT NULL,PRIMARY KEY(id),KEY user_id_id (user_id,id)) ENGINE=InnoDB'),
    'create tweet table',
);
ok(
    $dbh->do('CREATE TABLE incline_follow (followee INT UNSIGNED NOT NULL,follower INT UNSIGNED NOT NULL,PRIMARY KEY(followee,follower)) ENGINE=InnoDB'),
    'create cal_member table',
);
ok(
    $dbh->do('CREATE TABLE incline_timeline (user_id INT UNSIGNED NOT NULL,tweet_id INT UNSIGNED NOT NULL,PRIMARY KEY(user_id,tweet_id)) ENGINE=InnoDB'),
    'create cal_by_user table',
);

# load rules
system(qw(src/incline --source=example/multimaster.json --mysql-port=19010 --database=test create-trigger)) == 0
    or die "src/incline failed: $?";

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
    $dbh->do('INSERT INTO incline_tweet (user_id,body) VALUES (1,"hello")'),
    'tweet',
);
is_deeply($cmpf->(), 'post tweet check');
ok(
    $dbh->do('INSERT INTO incline_tweet (user_id,body) VALUES (2,"ciao")'),
    'tweet 2',
);
is_deeply($cmpf->(), 'post tweet check 2');
ok(
    $dbh->do('INSERT INTO incline_tweet (user_id,body) VALUES (3,"hola")'),
    'tweet 3',
);
is_deeply($cmpf->(), 'post tweet check 3');
ok(
    $dbh->do('INSERT INTO incline_follow (followee,follower) VALUES (2,4),(1,5)'),
    'add relation',
);
is_deeply($cmpf->(), 'post relation addition check');
ok(
    $dbh->do('DELETE FROM incline_tweet WHERE user_id=1 LIMIT 1'),
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
