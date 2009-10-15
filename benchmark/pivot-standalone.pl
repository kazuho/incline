#! /usr/bin/perl

use strict;
use warnings;

use lib qw(t);

use Benchmark ();
use DBI;
use InclineTest;

my $ROWS = 10_000;
my $ROWS_PER_STMT = 100;

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
my $dbh = DBI->connect($db->dsn)
    or die $DBI::errstr;
$dbh->do('CREATE TABLE d (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y),UNIQUE (y,x))')
    or die $dbh->errstr;
$dbh->do('CREATE TABLE i_s (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y))')
    or die $dbh->errstr;
$dbh->do('CREATE TABLE i_d (y INT NOT NULL,x INT NOT NULL,PRIMARY KEY (y,x))')
    or die $dbh->errstr;
system(
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--source=benchmark/pivot-standalone-def.json --port=19010 --database=test
       create-trigger),
) == 0 or die "src/incline failed: $?";

sub do_insert {
    my $table = shift;
    my @rows;
    for (my $i = 0; $i < $ROWS; $i++) {
        push @rows, "($i," . ($i * 33 % $ROWS) . ')';
        if (@rows == $ROWS_PER_STMT) {
            $dbh->do("INSERT INTO $table (x,y) VALUES " . join(',', @rows))
                or die $dbh->errstr;
            @rows = ();
        }
    }
}

sub do_delete {
    my $table = shift;
    for (my $i = 0; $i < $ROWS; $i += $ROWS_PER_STMT) {
        $dbh->do(
            "DELETE FROM $table WHERE $i<=x AND x<" . ($i + $ROWS_PER_STMT),
        ) or die $dbh->errstr;
    }
}

my %bench;
# benchmark
for (my $i = 0; $i < 1; $i++) {
    # direct
    for (1, 10, 100) {
        $ROWS_PER_STMT = $_;
        print STDERR ".";
        push_bench(
            ($bench{"insert ($ROWS_PER_STMT rows)"} ||= {}),
            'direct',
            sub { do_insert('d') },
        );
        print STDERR ".";
        push_bench(
            ($bench{"delete ($ROWS_PER_STMT rows)"} ||= {}),
            'direct',
            sub { do_delete('d') },
        );
        $dbh->selectrow_arrayref('SELECT COUNT(*) FROM d')->[0] == 0
            or die "logic flaw";
        # incline
        print STDERR ".";
        push_bench(
            ($bench{"insert ($ROWS_PER_STMT rows)"} ||= {}),
            'incline',
            sub { do_insert('i_s') },
        );
        $dbh->selectrow_arrayref('SELECT COUNT(*) FROM i_d')->[0] == $ROWS
            or die "logic flaw";
        print STDERR ".";
        push_bench(
            ($bench{"delete ($ROWS_PER_STMT rows)"} ||= {}),
            'incline',
            sub { do_delete('i_s') },
        );
        $dbh->selectrow_arrayref('SELECT COUNT(*) FROM i_s')->[0] == 0
            or die "logic flaw";
        $dbh->selectrow_arrayref('SELECT COUNT(*) FROM i_d')->[0] == 0
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

$dbh->disconnect;
undef $dbh;
undef $db;
