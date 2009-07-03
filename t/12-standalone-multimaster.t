use strict;
use warnings;

use DBI;
use Test::More tests => 23;

my $dbh = DBI->connect('dbi:mysql:test;user=root')
    or die DBI->errstr;

# create tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/canal_tweet canal_follow canal_timeline/;
ok(
    $dbh->do('CREATE TABLE canal_tweet (id INT UNSIGNED NOT NULL AUTO_INCREMENT,user_id INT UNSIGNED NOT NULL,body VARCHAR(255) NOT NULL,PRIMARY KEY(id),KEY user_id_id (user_id,id))'),
    'create tweet table',
);
ok(
    $dbh->do('CREATE TABLE canal_follow (followee INT UNSIGNED NOT NULL,follower INT UNSIGNED NOT NULL,PRIMARY KEY(followee,follower))'),
    'create cal_member table',
);
ok(
    $dbh->do('CREATE TABLE canal_timeline (user_id INT UNSIGNED NOT NULL,tweet_id INT UNSIGNED NOT NULL,PRIMARY KEY(user_id,tweet_id))'),
    'create cal_by_user table',
);

# load rules
system(qw(src/incline --source=example/standalone-multimaster.json --database=test create-trigger)) == 0
    or die "src/incline failed: $?";

# run tests
my $cmpf = sub {
    return (
        $dbh->selectall_arrayref('SELECT follow.follower,tweet.id,tweet.user_id,tweet.body FROM canal_tweet AS tweet INNER JOIN canal_follow AS follow ON follow.followee=tweet.user_id ORDER BY follow.follower,tweet.id'),
        $dbh->selectall_arrayref('SELECT timeline.user_id,tweet.id,tweet.user_id,tweet.body FROM canal_tweet AS tweet INNER JOIN canal_timeline AS timeline ON timeline.tweet_id=tweet.id ORDER BY timeline.user_id,tweet.id'),
    );
};
ok(
    $dbh->do('INSERT INTO canal_follow (followee,follower) VALUES (1,2),(2,1),(1,3),(1,4)'),
    'setup relations',
);
is_deeply($cmpf->(), 'post relations setup check');
ok(
    $dbh->do('INSERT INTO canal_tweet (user_id,body) VALUES (1,"hello")'),
    'tweet',
);
is_deeply($cmpf->(), 'post tweet check');
ok(
    $dbh->do('INSERT INTO canal_tweet (user_id,body) VALUES (2,"ciao")'),
    'tweet 2',
);
is_deeply($cmpf->(), 'post tweet check 2');
ok(
    $dbh->do('INSERT INTO canal_tweet (user_id,body) VALUES (3,"hola")'),
    'tweet 3',
);
is_deeply($cmpf->(), 'post tweet check 3');
ok(
    $dbh->do('INSERT INTO canal_follow (followee,follower) VALUES (2,4),(1,5)'),
    'add relation',
);
is_deeply($cmpf->(), 'post relation addition check');
ok(
    $dbh->do('DELETE FROM canal_tweet WHERE user_id=1 LIMIT 1'),
    'delete one tweet',
);
is_deeply($cmpf->(), 'post tweet deletion check');
ok(
    $dbh->do('DELETE FROM canal_follow WHERE followee=2 AND follower=1'),
    'delete relation',
);
is_deeply($cmpf->(), 'post relation deletion check');

# drop tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/canal_cal canal_cal_member canal_cal_by_user/;
