use strict;
use warnings;

use DBI;
use Scope::Guard;
use Test::mysqld;

use Test::More tests => 66;

my @incline_cmd = qw(src/incline --mode=shard --source=example/shard-singlemaster.json --shard-source=example/shard-range.json --database=test);
my @db_nodes = (
    qw/127.0.0.1:19010 127.0.0.1:19011 127.0.0.1:19012/, # use the first three
);
my @mysqld;
my @dbi_uri;
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
    push @dbi_uri, "dbi:mysql:test;user=root;host=$db_host;port=$db_port";
    # create tables
    push @dbh, do {
        my $dbh = DBI->connect($dbi_uri[-1])
            or die DBI->errstr;
        $dbh;
    };
    ok($dbh[-1]->do("DROP TABLE IF EXISTS $_"), "drop $_")
        for qw/incline_cal incline_cal_member incline_cal_by_user/;
    ok(
        $dbh[-1]->do('CREATE TABLE incline_cal (id INT UNSIGNED NOT NULL,at INT UNSIGNED NOT NULL,title VARCHAR(255) NOT NULL,PRIMARY KEY(id),KEY at(at)) ENGINE=InnoDB'),
        'create cal table',
    );
    ok(
        $dbh[-1]->do('CREATE TABLE incline_cal_member (cal_id INT UNSIGNED NOT NULL,user_id INT UNSIGNED NOT NULL,PRIMARY KEY(cal_id,user_id)) ENGINE=InnoDB'),
        'create cal_member table',
    );
    ok(
        $dbh[-1]->do('CREATE TABLE incline_cal_by_user (_user_id INT UNSIGNED NOT NULL,_cal_id INT UNSIGNED NOT NULL,_at INT UNSIGNED NOT NULL,PRIMARY KEY(_user_id,_cal_id),KEY user_id_at_cal_id (_user_id,_at,_cal_id)) ENGINE=InnoDB'),
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
    my $waitf = sub {
        while (1) {
            my $rows = $dbh[0]->selectall_arrayref(
                'SELECT COUNT(*) FROM _iq_incline_cal_by_user',
            ) or die $dbh[0]->errstr;
            return if $rows->[0]->[0] == 0;
            sleep 1;
        }
    };
    my $cmpf = sub {
        $waitf->();
        return (
            $dbh[0]->selectall_arrayref('SELECT user_id,cal_id,at FROM incline_cal INNER JOIN incline_cal_member ON incline_cal.id=incline_cal_member.cal_id ORDER BY user_id,cal_id'),
            $dbh[0]->selectall_arrayref('SELECT _user_id,_cal_id,_at FROM incline_cal_by_user ORDER BY _user_id,_cal_id'),
        );
    };
    ok(
        $dbh[0]->do('INSERT INTO incline_cal (id,at,title) VALUES (1,9,"hello")'),
        'insert into cal',
    );
    ok(
        $dbh[0]->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (1,101),(1,102)'),
        'insert into cal_member',
    );
    is_deeply($cmpf->(), 'post insertion check');
    ok(
        $dbh[0]->do('UPDATE incline_cal SET at=at+1'),
        'update dependent table',
    );
    is_deeply($cmpf->(), 'post deletion from master check');
    ok(
        $dbh[0]->do('DELETE FROM incline_cal_member WHERE user_id=102'),
        'delete from master',
    );
    is_deeply($cmpf->(), 'post deletion from master check');
}

# delete all rows for async test
ok(
    $dbh[0]->do('DELETE FROM incline_cal_member'),
    'clear cal_member',
);
ok(
    $dbh[0]->do('DELETE FROM incline_cal'),
    'clear cal_member',
);

# check that changes sent to be a different node is not applied
ok(
    $dbh[0]->do('INSERT INTO incline_cal (id,at,title) VALUES (2,99,"ciao")'),
    'insert into cal',
);
ok(
    $dbh[0]->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (2,1000),(2,1001)'),
    'insert into cal_member (1000,1001)',
);
is_deeply(
    $dbh[0]->selectall_arrayref('SELECT _cal_id,_user_id FROM _iq_incline_cal_by_user ORDER BY _user_id'),
    [ [ 2, 1000 ], [ 2, 1001 ] ],
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
        my $target = shift;
        while (1) {
            my $rows = $dbh[0]->selectall_arrayref(
                'SELECT COUNT(*) FROM _iq_incline_cal_by_user WHERE ?<=_user_id AND _user_id<?',
                {},
                1000 * $target,
                1000 * ($target + 1),
            ) or die $dbh[0]->errstr;
            return if $rows->[0]->[0] == 0;
            sleep 1;
        }
    };
    my $cmpf = sub {
        my $target = shift;
        $waitf->($target);
        return (
            $dbh[0]->selectall_arrayref(
                'SELECT user_id,cal_id,at FROM incline_cal INNER JOIN incline_cal_member ON incline_cal.id=incline_cal_member.cal_id WHERE ?<=incline_cal_member.user_id AND incline_cal_member.user_id<? ORDER BY user_id,cal_id',
                {},
                1000 * $target,
                1000 * ($target + 1),
            ),
            $dbh[$target]->selectall_arrayref('SELECT _user_id,_cal_id,_at FROM incline_cal_by_user ORDER BY _user_id,_cal_id'),
        );
    };
    is_deeply($cmpf->(1), 'post insertion check');
    ok(
        $dbh[0]->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (2,1002)'),
        'insert into cal_member (1002)',
    );
    is_deeply($cmpf->(1), 'post insertion check (1002)');
    ok(
        $dbh[0]->do('UPDATE incline_cal SET at=at+1 WHERE id=2'),
        'update cal.at',
    );
    is_deeply($cmpf->(1), 'post update check');
    ok(
        $dbh[0]->do('DELETE FROM incline_cal_member WHERE cal_id=2 AND user_id=1000'),
        'delete from cal_member',
    );
    is_deeply($cmpf->(1), 'post delete check');
    # partially down test
    undef $dbh[2];
    $mysqld[2]->stop();
    ok(
        $dbh[0]->do(
            'INSERT INTO incline_cal (id,at,title) VALUES (3,99,"hola")',
        ),
        'insert into cal (partially down)',
    );
    ok(
        $dbh[0]->do(
            'INSERT INTO incline_cal_member (cal_id,user_id) VALUES (3,1000),(3,2000)'),
        'insert into cal_member (partially down)',
    );
    is_deeply($cmpf->(1), 'post insert check (partially down)');
    ok(
        $dbh[0]->do(
            'INSERT INTO incline_cal_member (cal_id,user_id) VALUES (3,1001)',
        ),
        'insert into cal_member 2 (partially down)',
    );
    $mysqld[2]->start();
    $dbh[2] = DBI->connect($dbi_uri[2])
        or die $DBI::errstr;
    is_deeply($cmpf->(2), 'recovery test');
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
        for qw/incline_cal incline_cal_member incline_cal_by_user/;
}
