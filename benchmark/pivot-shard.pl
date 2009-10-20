#! /usr/bin/perl

use strict;
use warnings;

use lib qw(t);

use Benchmark ();
use DBI;
use List::Util qw(sum);
use Time::HiRes qw(sleep);

use InclineTest;

our $ROWS = $ENV{ROWS} || 100_000;
our $NUM_WORKERS = $ENV{NUM_WORKERS} || 10;
die "condition check failed"
    unless $ROWS % $NUM_WORKERS == 0;

# setup
my @db;
for my $port (19010..19014) {
    push @db, init_db(
        mysqld => {
            my_cnf => {
                'bind-address'           => '127.0.0.1',
                port                     => $port,
                'default-storage-engine' => 'INNODB',
                innodb_buffer_pool_size  => '128M',
                innodb_flush_method      => 'O_DIRECT',
                innodb_log_file_size     => '32M',
            },
        },
        postgresql => {
            port => $port,
        },
    );
    my $dbh = DBI->connect($db[-1]->dsn, undef, undef, { AutoCommit => 1 })
        or die $DBI::errstr;
    $dbh->do('CREATE TABLE dx (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y))')
        or die $dbh->errstr;
    $dbh->do('CREATE TABLE dy (y INT NOT NULL,x INT NOT NULL,PRIMARY KEY (y,x))')
        or die $dbh->errstr;
    $dbh->do('CREATE TABLE ix (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y))')
        or die $dbh->errstr;
    $dbh->do('CREATE TABLE iy (y INT NOT NULL,x INT NOT NULL,PRIMARY KEY (y,x))')
        or die $dbh->errstr;
    my @cmd = (
        qw(src/incline --mode=shard --database=test --host=127.0.0.1
           --source=benchmark/pivot-shard-def.json
           --shard-source=benchmark/shard-db.json
           ),
        "--rdbms=$ENV{TEST_DBMS}",
        "--port=$port",
    );
    system(@cmd, qw(create-queue)) == 0
        or die "src/incline create-queue failed: $?";
    system(@cmd, qw(create-trigger)) == 0
        or die "src/incline create-trigger failed: $?";
    unless (my $pid = fork) {
        die "fork failed:$!"
            unless defined $pid;
        exec(@cmd, qw(forward))
            or die "src/incline forward failed: $?";
    }
}

sub doit (&) {
    my $cb = shift;
    die "condition check failed"
        unless $ROWS % $NUM_WORKERS == 0 && $NUM_WORKERS % @db == 0;
    do_parallel(
        $NUM_WORKERS,
        sub {
            my $x = shift;
            for (1 .. ($ROWS / $NUM_WORKERS)) {
                $cb->($x, $x * 33 % $ROWS);
                $x += $NUM_WORKERS;
            }
        },
    );
}

sub dbh_for {
    my ($dbh, $v) = @_;
    $dbh->[$v % @db] ||= DBI->connect(
        $db[$v % @db]->dsn, undef, undef, { AutoCommit => 1 },
    ) or die $DBI::errstr;
}

my %bench;
# benchmark
for (my $i = 0; $i < 3; $i++) {
    # direct
    print STDERR ".";
    push_bench(
        ($bench{"insert (1 row)"} ||= {}),
        'direct',
        sub {
            my @dbh;
            doit sub {
                my ($x, $y) = @_;
                my ($xh, $yh) = (dbh_for(\@dbh, $x), dbh_for(\@dbh, $y));
                if ($xh == $yh) {
                    $xh->begin_work
                        or die $xh->errstr;
                }
                $xh->do("INSERT INTO dx (x,y) VALUES ($x,$y)")
                    or die $xh->errstr;
                $yh->do("INSERT INTO dy (y,x) VALUES ($y,$x)")
                    or die $yh->errstr;
                if ($xh == $yh) {
                    $xh->commit
                        or die $xh->errstr;
                }
            };
        },
    );
    print STDERR ".";
    push_bench(
        ($bench{"delete (1 row)"} ||= {}),
        'direct',
        sub {
            my @dbh;
            doit sub {
                my ($x, $y) = @_;
                my ($xh, $yh) = (dbh_for(\@dbh, $x), dbh_for(\@dbh, $y));
                if ($xh == $yh) {
                    $xh->begin_work
                        or die $xh->errstr;
                }
                $xh->do("DELETE FROM dx WHERE x=$x AND y=$y")
                    or die $xh->errstr;
                $yh->do("DELETE FROM dy WHERE y=$y AND x=$x")
                    or die $yh->errstr;
                if ($xh == $yh) {
                    $xh->commit
                        or die $xh->errstr;
                }
            };
        },
    );
    for my $tbl (qw(dx dy)) {
        sum(
            map {
                DBI->connect($_->dsn)->selectrow_arrayref(
                    "SELECT COUNT(*) FROM $tbl",
                )->[0]
            } @db,
        ) == 0
            or die "logic flaw";
    }
    # incline
    print STDERR ".";
    push_bench(
        ($bench{"insert (1 row)"} ||= {}),
        'incline',
        sub {
            my @dbh;
            doit sub {
                my ($x, $y) = @_;
                map {
                    $_->do("INSERT INTO ix (x,y) VALUES ($x,$y)")
                        or die $_->errstr;
                } dbh_for(\@dbh, $x);
            };
            while (1) {
                sum(
                    map {
                        DBI->connect($_->dsn)->selectrow_arrayref(
                            'SELECT COUNT(*) FROM iy',
                        )->[0]
                    } @db,
                ) == $ROWS
                    and last;
                sleep(0.01);
            }
        },
    );
    print STDERR ".";
    push_bench(
        ($bench{"delete (1 row)"} ||= {}),
        'incline',
        sub {
            my @dbh;
            doit sub {
                my ($x, $y) = @_;
                map {
                    $_->do("DELETE FROM ix WHERE x=$x AND y=$y")
                        or die $_->errstr;
                } dbh_for(\@dbh, $x);
            };
            while (1) {
                sum(
                    map {
                        DBI->connect($_->dsn)->selectrow_arrayref(
                            'SELECT COUNT(*) FROM iy',
                        )->[0]
                    } @db,
                ) == 0
                    and last;
                sleep(0.01);
            }
        },
    );
}

print STDERR "\n\n";

# print
for my $n (sort keys %bench) {
    print "$n:\n";
    Benchmark::cmpthese($bench{$n});
    print "\n";
}

undef @db;
