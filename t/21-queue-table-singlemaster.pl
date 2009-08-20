use strict;
use warnings;

use lib qw(t);

use DBI;
use InclineTest;
use Scope::Guard;
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

plan tests => 21;

my @incline_cmd = (
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--mode=queue-table --port=19010 --source=example/singlemaster.json),
    qw(--database=test),
);

my $dbh = InclineTest->connect(
    'DBI:any(PrintWarn=>0,RaiseError=>0):dbname=test;user=root;host=127.0.0.1;port=19010',
) or die DBI->errstr;

# create tables
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_cal incline_cal_member incline_cal_by_user/;
ok(
    $dbh->do('CREATE TABLE incline_cal (id INT NOT NULL,at_time INT NOT NULL,title VARCHAR(255) NOT NULL,PRIMARY KEY(id))'),
    'create cal table',
);
ok(
    $dbh->do('CREATE TABLE incline_cal_member (cal_id INT NOT NULL,user_id INT NOT NULL,PRIMARY KEY(cal_id,user_id))'),
    'create cal_member table',
);
ok(
    $dbh->do('CREATE TABLE incline_cal_by_user (_user_id INT NOT NULL,_cal_id INT NOT NULL,_at_time INT NOT NULL,PRIMARY KEY(_user_id,_cal_id))'),
    'create cal_by_user table',
);

# load rules
ok(system(@incline_cmd, 'drop-queue') == 0, 'drop queue if exists');
ok(system(@incline_cmd, 'create-queue') == 0, 'create queue');
ok(system(@incline_cmd, 'create-trigger') == 0, 'create queue');

{
    # run forwarder
    my ($fw_script, $fw_pid);
    my $_sg = Scope::Guard->new(
        sub {
            kill 9, $fw_pid
                if $fw_pid;
        },
    );
    unless ($fw_pid = fork()) {
        exec(@incline_cmd, 'forward');
        die "failed to exec forwarder: $?";
    }
    
    # run tests
    my $waitf = sub {
        while (1) {
            my $rows = $dbh->selectall_arrayref(
                'SELECT COUNT(*) FROM _iq_incline_cal_by_user',
            ) or die $dbh->errstr;
            return if $rows->[0]->[0] == 0;
            sleep 1;
        }
    };
    my $cmpf = sub {
        $waitf->();
        return (
            $dbh->selectall_arrayref('SELECT user_id,cal_id,at_time FROM incline_cal INNER JOIN incline_cal_member ON incline_cal.id=incline_cal_member.cal_id ORDER BY user_id,cal_id'),
            $dbh->selectall_arrayref('SELECT _user_id,_cal_id,_at_time FROM incline_cal_by_user ORDER BY _user_id,_cal_id'),
        );
    };
    ok(
        $dbh->do(q{INSERT INTO incline_cal (id,at_time,title) VALUES (1,999,'hello')}),
        'insert into cal',
    );
    ok(
        $dbh->do('INSERT INTO incline_cal_member (cal_id,user_id) VALUES (1,101),(1,102)'),
        'insert into cal_member',
    );
    is_deeply($cmpf->(), 'post insertion check');
    ok(
        $dbh->do('UPDATE incline_cal SET at_time=at_time+1'),
        'update dependent table',
    );
    is_deeply($cmpf->(), 'post deletion from master check');
    ok(
        $dbh->do('DELETE FROM incline_cal_member WHERE user_id=102'),
        'delete from master',
    );
    is_deeply($cmpf->(), 'post deletion from master check');
}

sleep 1;

# drop tables
ok(system(@incline_cmd, 'drop-trigger') == 0, 'drop queue if exists');
ok(system(@incline_cmd, 'drop-queue') == 0, 'drop queue');
ok($dbh->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_cal incline_cal_member incline_cal_by_user/;

1;
