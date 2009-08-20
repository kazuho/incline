use strict;
use warnings;

use lib qw(t);

use DBI;
use InclineTest;
use Scope::Guard;
use Test::More;

# skip tests if dbms does not exist
InclineTest->create_any(
    mysqld => {
        my_cnf => { 'skip-networking' => '' },
    },
    postgresql => {},
);

plan tests => 47;

my @incline_cmd = (
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--mode=shard --source=example/shard-singlemaster.json),
    qw(--shard-source=example/shard-hash.json --database=test),
);
my @db_nodes = qw/127.0.0.1:19010 127.0.0.1:19011/; # only use the first two
my @db;
my @dbh;

for my $db_node (@db_nodes) {
    my ($db_host, $db_port) = split /:/, $db_node, 2;
    # start db
    push @db, InclineTest->create_any(
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
        my $dbh = InclineTest->connect(
            "DBI:any(PrintWarn=>0,RaiseError=>0):dbname=test;user=root;host=$db_host;port=$db_port",
        ) or die DBI->errstr;
        $dbh;
    };
    ok($dbh[-1]->do("DROP TABLE IF EXISTS $_"), "drop $_")
        for qw/incline_cal incline_cal_member incline_cal_by_user/;
    ok(
        $dbh[-1]->do('CREATE TABLE incline_cal (id INT NOT NULL,at_time INT NOT NULL,title VARCHAR(255) NOT NULL,PRIMARY KEY(id))'),
        'create cal table',
    );
    ok(
        $dbh[-1]->do('CREATE TABLE incline_cal_member (cal_id INT NOT NULL,user_id INT NOT NULL,PRIMARY KEY(cal_id,user_id))'),
        'create cal_member table',
    );
    ok(
        $dbh[-1]->do('CREATE TABLE incline_cal_by_user (_user_id INT NOT NULL,_cal_id INT NOT NULL,_at_time INT NOT NULL,PRIMARY KEY(_user_id,_cal_id))'),
        'create cal_by_user table',
    );
    # load rules
    ok(
        system(
            @incline_cmd,
            "--host=$db_host",
            "--port=$db_port",
            'drop-queue',
        ) == 0,
        'drop queue if exists',
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
            $dbh[0]->selectall_arrayref('SELECT user_id,cal_id,at_time FROM incline_cal INNER JOIN incline_cal_member ON incline_cal.id=incline_cal_member.cal_id ORDER BY user_id,cal_id'),
            $dbh[0]->selectall_arrayref('SELECT _user_id,_cal_id,_at_time FROM incline_cal_by_user ORDER BY _user_id,_cal_id'),
        );
    };
    ok(
        $dbh[0]->do(q{INSERT INTO incline_cal (id,at_time,title) VALUES (1,9,'hello')}),
        'insert into cal',
    );
    ok(
        $dbh[0]->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (1,4),(1,8)'),
        'insert into cal_member',
    );
    is_deeply($cmpf->(), 'post insertion check');
    ok(
        $dbh[0]->do('UPDATE incline_cal SET at_time=at_time+1'),
        'update dependent table',
    );
    is_deeply($cmpf->(), 'post deletion from master check');
    ok(
        $dbh[0]->do('DELETE FROM incline_cal_member WHERE user_id=8'),
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
    $dbh[0]->do(q{INSERT INTO incline_cal (id,at_time,title) VALUES (2,99,'ciao')}),
    'insert into cal',
);
ok(
    $dbh[0]->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (2,5),(2,9)'),
    'insert into cal_member (1000,1001)',
);
is_deeply(
    $dbh[0]->selectall_arrayref('SELECT _cal_id,_user_id FROM _iq_incline_cal_by_user ORDER BY _user_id'),
    [ [ 2, 5 ], [ 2, 9 ] ],
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
            "--host=$db_host",
            "--port=$db_port",
            'forward',
        );
        die "failed to exec forwarder: $?";
    }
    # tests
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
            $dbh[0]->selectall_arrayref('SELECT user_id,cal_id,at_time FROM incline_cal INNER JOIN incline_cal_member ON incline_cal.id=incline_cal_member.cal_id ORDER BY user_id,cal_id'),
            $dbh[1]->selectall_arrayref('SELECT _user_id,_cal_id,_at_time FROM incline_cal_by_user ORDER BY _user_id,_cal_id'),
        );
    };
    is_deeply($cmpf->(), 'post insertion check');
    ok(
        $dbh[0]->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (2,13)'),
        'insert into cal_member (13)',
    );
    is_deeply($cmpf->(), 'post insertion check (13)');
    ok(
        $dbh[0]->do('UPDATE incline_cal SET at_time=at_time+1 WHERE id=2'),
        'update cal.at_time',
    );
    is_deeply($cmpf->(), 'post update check');
    ok(
        $dbh[0]->do('DELETE FROM incline_cal_member WHERE cal_id=2 AND user_id=5'),
        'delete from cal_member',
    );
    is_deeply($cmpf->(), 'post delete check');
}
sleep 1;

# drop tables
for my $db_node (@db_nodes) {
    my ($db_host, $db_port) = split /:/, $db_node, 2;
    ok(
        system(
            @incline_cmd,
            "--host=$db_host",
            "--port=$db_port",
            'drop-trigger',
        ) == 0,
        'drop trigger',
    );
    ok(
        system(
            @incline_cmd,
            "--host=$db_host",
            "--port=$db_port",
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

1;
