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

plan tests => 19;

my @incline_cmd = (
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--mode=queue-table --port=19010 --source=example/single.json),
    qw(--database=test),
);

my $_dbh;

sub dbh {
    $_dbh ||= InclineTest->connect(
        'DBI:any(PrintWarn=>0,RaiseError=>0):dbname=test;user=root;host=127.0.0.1;port=19010'
    ) or die DBI->errstr;
    $_dbh;
}

sub dbh_close {
    $_dbh->disconnect;
    undef $_dbh;
}

# create tables
ok(dbh()->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_dest incline_src/;
ok(
    dbh()->do('CREATE TABLE incline_dest (_id INT NOT NULL,_message VARCHAR(255) NOT NULL,PRIMARY KEY(_id))'),
    'create dest table',
);
ok(
    dbh()->do(
        InclineTest->adjust_ddl(
            'CREATE TABLE incline_src (id SERIAL,message VARCHAR(255) NOT NULL,PRIMARY KEY (id))',
        ),
    ),
    'create src table',
);

# load rules
ok(system(@incline_cmd, 'drop-queue') == 0, 'drop queue if exists');
ok(system(@incline_cmd, 'create-queue') == 0, 'create queue');
ok(system(@incline_cmd, 'create-trigger') == 0, 'create queue');

{
    # run forwarder
    my $fw_pid;
    my $_sg = Scope::Guard->new(
        sub {
            kill 9, $fw_pid
                if $fw_pid;
        },
    );
    dbh_close();
    unless ($fw_pid = fork()) {
        exec(@incline_cmd, 'forward');
        die "failed to exec forwarder: $?";
    }
    
    # run tests
    my $waitf = sub {
        while (1) {
            my $rows = dbh()->selectall_arrayref(
                'SELECT COUNT(*) FROM _iq_incline_dest',
            ) or die dbh()->errstr;
            return if $rows->[0]->[0] == 0;
            sleep 1;
        }
    };
    my $cmpf = sub {
        $waitf->();
        return (
            dbh()->selectall_arrayref('SELECT * FROM incline_src ORDER BY id'),
            dbh()->selectall_arrayref('SELECT * FROM incline_dest ORDER BY _id'),
        );
    };
    ok(
        dbh()->do(q{INSERT INTO incline_src (message) VALUES ('hello')}),
        'insert',
    );
    is_deeply($cmpf->(), 'post insertion check');
    ok(
        dbh()->do(
            q{INSERT INTO incline_src (message) VALUES ('hello'),('ciao')},
        ),
        'insert',
    );
    is_deeply($cmpf->(), 'post insertion check');
    ok(
        dbh()->do(q{UPDATE incline_src SET message='good bye' WHERE id%2!=0}),
        'update',
    );
    is_deeply($cmpf->(), 'post update check');
    ok(
        dbh()->do('DELETE FROM incline_src WHERE id%2=0'),
        'delete',
    );
    is_deeply($cmpf->(), 'post delete check');
}

sleep 1;

# drop tables
ok(system(@incline_cmd, 'drop-trigger') == 0, 'drop queue if exists');
ok(system(@incline_cmd, 'drop-queue') == 0, 'drop queue');
ok(dbh()->do("DROP TABLE IF EXISTS $_"), "drop $_")
    for qw/incline_dest incline_src/;

dbh_close();

1;
