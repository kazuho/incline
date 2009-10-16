#! /usr/bin/perl

use strict;
use warnings;

use lib qw(t);

use Benchmark ();
use DBI;
use InclineTest;

my $ROWS = 10_000;
my $NUM_WORKERS = 10;

# setup
my $db = init_db(
    mysqld => {
        my_cnf => {
            'bind-address'                 => '127.0.0.1',
            port                           => 19010,
            'default-storage-engine'       => 'INNODB',
            innodb_buffer_pool_size        => '128M',
            innodb_flush_method            => 'O_DIRECT',
            innodb_log_file_size           => '128M',
        },
    },
    postgresql => {
        port => 19010,
    },
);
{
    my $dbh = DBI->connect($db->dsn)
        or die $DBI::errstr;
    $dbh->do('CREATE TABLE d (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y),UNIQUE (y,x))')
        or die $dbh->errstr;
    $dbh->do('CREATE TABLE i_s (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y))')
        or die $dbh->errstr;
    $dbh->do('CREATE TABLE i_d (y INT NOT NULL,x INT NOT NULL,PRIMARY KEY (y,x))')
        or die $dbh->errstr;
}
system(
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--source=benchmark/pivot-standalone-def.json --port=19010 --database=test
       create-trigger),
) == 0 or die "src/incline failed: $?";

sub do_parallel {
    my $code = shift;
    my %pids;
    for (my $i = 0; $i < $NUM_WORKERS; $i++) {
        defined(my $pid = fork)
            or die "fork failed:$!";
        if ($pid == 0) {
            # child process
            $code->($i);
            exit 0;
        }
        $pids{$pid} = 1;
    }
    while (%pids) {
        if ((my $pid = wait) != -1) {
            delete $pids{$pid};
        }
    }
}

sub do_insert {
    my ($table, $rows_per_stmt) = @_;
    do_parallel(
        sub {
            my $base = shift(@_) * $ROWS / $NUM_WORKERS;
            my $dbh = DBI->connect($db->dsn)
                or die $DBI::errstr;
            my @rows;
            for (my $i = 0; $i < $ROWS / $NUM_WORKERS; $i++) {
                my $x = $i + $base;
                push @rows, "($x," . ($x * 33 % $ROWS) . ')';
                if (@rows == $rows_per_stmt) {
                    $dbh->do(
                        "INSERT INTO $table (x,y) VALUES " . join(',', @rows),
                    ) or die $dbh->errstr;
                    @rows = ();
                }
            }
        },
    );
}

sub do_delete {
    my ($table, $rows_per_stmt) = @_;
    do_parallel(
        sub {
            my $base = shift(@_) * $ROWS / $NUM_WORKERS;
            my $dbh = DBI->connect($db->dsn)
                or die $DBI::errstr;
            for (my $i = 0; $i < $ROWS / $NUM_WORKERS; $i += $rows_per_stmt) {
                my $x = $i + $base;
                $dbh->do(
                    "DELETE FROM $table WHERE $x<=x AND x<"
                        . ($x + $rows_per_stmt),
                ) or die $dbh->errstr;
            }
        },
    );
}

my %bench;
# benchmark
for (my $i = 0; $i < 1; $i++) {
    # direct
    for my $rows_per_stmt (1, 10, 100) {
        die "condition check failed"
            unless $ROWS % ($NUM_WORKERS * $rows_per_stmt) == 0;
        print STDERR ".";
        push_bench(
            ($bench{"insert ($rows_per_stmt rows)"} ||= {}),
            'direct',
            sub { do_insert('d', $rows_per_stmt) },
        );
        print STDERR ".";
        push_bench(
            ($bench{"delete ($rows_per_stmt rows)"} ||= {}),
            'direct',
            sub { do_delete('d', $rows_per_stmt) },
        );
        DBI->connect($db->dsn)->selectrow_arrayref(
            'SELECT COUNT(*) FROM d',
        )->[0] == 0
            or die "logic flaw";
        # incline
        print STDERR ".";
        push_bench(
            ($bench{"insert ($rows_per_stmt rows)"} ||= {}),
            'incline',
            sub { do_insert('i_s', $rows_per_stmt) },
        );
        DBI->connect($db->dsn)->selectrow_arrayref(
            'SELECT COUNT(*) FROM i_d',
        )->[0] == $ROWS
            or die "logic flaw";
        print STDERR ".";
        push_bench(
            ($bench{"delete ($rows_per_stmt rows)"} ||= {}),
            'incline',
            sub { do_delete('i_s', $rows_per_stmt) },
        );
        DBI->connect($db->dsn)->selectrow_arrayref(
            'SELECT COUNT(*) FROM i_s',
        )->[0] == 0
            or die "logic flaw";
        DBI->connect($db->dsn)->selectrow_arrayref(
            'SELECT COUNT(*) FROM i_d',
        )->[0] == 0
            or die "logic flaw";
    }
}

print STDERR "\n\n";

# print
for my $n (sort keys %bench) {
    print "$n:\n";
    Benchmark::cmpthese($bench{$n});
    print "\n";
}

undef $db;
