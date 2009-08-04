use strict;
use warnings;

use DBI;
use Scope::Guard;
use Test::mysqld;

use Test::More tests => 51;

my @incline_cmd = qw(src/incline --mode=shard --source=example/shard-multimaster.json --shard-source=example/shard-range.json --database=test);
my @db_nodes = qw/127.0.0.1:19010 127.0.0.1:19011/; # only use the first two
my @mysqld;
my @dbh;

for my $db_node (@db_nodes) {
    my ($db_host, $db_port) = split /:/, $db_node, 2;
    # start mysqld
    push @mysqld, Test::mysqld->new(
        my_cnf => {
            'bind-address' => $db_host,
            port           => $db_port,
        },
    );
    # create tables
    push @dbh, do {
        my $dbh = DBI->connect(
            "dbi:mysql:test;user=root;host=$db_host;port=$db_port",
        ) or die DBI->errstr;
        $dbh;
    };
    ok($dbh[-1]->do("DROP TABLE IF EXISTS $_"), "drop $_")
        for qw/incline_tweet incline_follow incline_timeline/;
    ok(
        $dbh[-1]->do('CREATE TABLE incline_tweet (id INT UNSIGNED NOT NULL AUTO_INCREMENT,user_id INT UNSIGNED NOT NULL,body VARCHAR(255) NOT NULL,PRIMARY KEY(id),KEY user_id_id (user_id,id)) ENGINE=InnoDB'),
        'create tweet table',
    );
    ok(
        $dbh[-1]->do('CREATE TABLE incline_follow (followee INT UNSIGNED NOT NULL,follower INT UNSIGNED NOT NULL,PRIMARY KEY(followee,follower)) ENGINE=InnoDB'),
        'create cal_member table',
    );
    ok(
        $dbh[-1]->do('CREATE TABLE incline_timeline (user_id INT UNSIGNED NOT NULL,tweet_id INT UNSIGNED NOT NULL,PRIMARY KEY(user_id,tweet_id)) ENGINE=InnoDB'),
        'create cal_by_user table',
    );
    # load rules
    ok(
        system(
            @incline_cmd,
            "--mysql-host=$db_host",
            "--mysql-port=$db_port",
            'drop-queue',
        ) == 0,
        'drop queue if exists',
    );
    ok(
        system(
            @incline_cmd,
            "--mysql-host=$db_host",
            "--mysql-port=$db_port",
            'create-queue',
        ) == 0,
        'create queue',
    );
    ok(
        system(
            @incline_cmd,
            "--mysql-host=$db_host",
            "--mysql-port=$db_port",
            'create-trigger',
        ) == 0,
        'create trigger',
    );
}

{ # check that changes are immediately applied on a single server
    my $cmpf = sub {
        return (
            $dbh[0]->selectall_arrayref('SELECT follow.follower,tweet.id,tweet.user_id,tweet.body FROM incline_tweet AS tweet INNER JOIN incline_follow AS follow ON follow.followee=tweet.user_id ORDER BY follow.follower,tweet.id'),
            $dbh[0]->selectall_arrayref('SELECT timeline.user_id,tweet.id,tweet.user_id,tweet.body FROM incline_tweet AS tweet INNER JOIN incline_timeline AS timeline ON timeline.tweet_id=tweet.id ORDER BY timeline.user_id,tweet.id'),
        );
    };
    ok(
        $dbh[0]->do('INSERT INTO incline_follow (followee,follower) VALUES (1,2),(2,1),(1,3),(1,4)'),
        'setup relations',
    );
    is_deeply($cmpf->(), 'post relations setup check');
    ok(
        $dbh[0]->do('INSERT INTO incline_tweet (user_id,body) VALUES (1,"hello")'),
        'tweet',
    );
    is_deeply($cmpf->(), 'post tweet check');
    ok(
        $dbh[0]->do('INSERT INTO incline_tweet (user_id,body) VALUES (2,"ciao")'),
        'tweet 2',
    );
    is_deeply($cmpf->(), 'post tweet check 2');
    ok(
        $dbh[0]->do('INSERT INTO incline_tweet (user_id,body) VALUES (3,"hola")'),
        'tweet 3',
    );
    is_deeply($cmpf->(), 'post tweet check 3');
    ok(
        $dbh[0]->do('INSERT INTO incline_follow (followee,follower) VALUES (2,4),(1,5)'),
        'add relation',
    );
    is_deeply($cmpf->(), 'post relation addition check');
    ok(
        $dbh[0]->do('DELETE FROM incline_tweet WHERE user_id=1 LIMIT 1'),
        'delete one tweet',
    );
    is_deeply($cmpf->(), 'post tweet deletion check');
    ok(
        $dbh[0]->do('DELETE FROM incline_follow WHERE followee=2 AND follower=1'),
        'delete relation',
    );
    is_deeply($cmpf->(), 'post relation deletion check');
}

# check that changes sent to be a different node is not applied
ok(
    $dbh[0]->do('INSERT INTO incline_follow (followee,follower) VALUES (1,1000),(2,1000),(2,1001)'),
    'insert into follower',
);
is_deeply(
    $dbh[0]->selectall_arrayref('SELECT user_id,tweet_id FROM _iq_incline_timeline ORDER BY user_id,tweet_id'),
    [ [ 1000, 2 ], [ 1001, 2 ] ],
    'check if queued',
);

{ # async tests
    # run forwarder
    my $fw_pid;
    my $_sg = Scope::Guard->new(
        sub {
            kill 9, $fw_pid;
        },
    );
    unless ($fw_pid = fork()) {
        my ($db_host, $db_port) = split /:/, $db_nodes[0], 2;
        exec(
            @incline_cmd,
            "--mysql-host=$db_host",
            "--mysql-port=$db_port",
            'forward',
        );
        die "failed to exec forwarder: $?";
    }
    # tests
    my $waitf = sub {
        while (1) {
            my $rows = $dbh[0]->selectall_arrayref(
                'SELECT COUNT(*) FROM _iq_incline_timeline',
            ) or die $dbh[0]->errstr;
            return if $rows->[0]->[0] == 0;
            sleep 1;
        }
    };
    my $cmpf = sub {
        $waitf->();
        return (
            $dbh[0]->selectall_arrayref('SELECT follow.follower,tweet.id FROM incline_tweet AS tweet INNER JOIN incline_follow AS follow ON follow.followee=tweet.user_id WHERE follow.follower>=1000 ORDER BY follow.follower,tweet.id'),
            $dbh[1]->selectall_arrayref('SELECT user_id,tweet_id FROM incline_timeline ORDER BY user_id,tweet_id'),
        );
    };
    is_deeply($cmpf->(), 'post insertion check');
    ok(
        $dbh[0]->do('INSERT INTO incline_tweet (user_id,body) VALUES (1,"hello again")'),
        'tweet',
    );
    is_deeply($cmpf->(), 'post tweet check');
    ok(
        $dbh[0]->do('DELETE FROM incline_follow WHERE followee=2 AND follower=1000'),
        'stop 1000 following 2',
    );
    is_deeply($cmpf->(), 'post stop following check');
    ok(
        $dbh[0]->do('DELETE FROM incline_tweet WHERE user_id=1'),
        'delete tweets of user 1',
    );
    is_deeply($cmpf->(), 'post tweet delete check');
}
sleep 1;

# drop tables
for my $db_node (@db_nodes) {
    my ($db_host, $db_port) = split /:/, $db_node, 2;
    ok(
        system(
            @incline_cmd,
            "--mysql-host=$db_host",
            "--mysql-port=$db_port",
            'drop-trigger',
        ) == 0,
        'drop trigger',
    );
    ok(
        system(
            @incline_cmd,
            "--mysql-host=$db_host",
            "--mysql-port=$db_port",
            'drop-queue',
        ) == 0,
        'drop queue',
    );
}
while (@dbh) {
    my $dbh = pop @dbh;
    ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
        for qw/incline_tweet incline_follow incline_timeline/;
}
