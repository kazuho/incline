#! /usr/bin/perl

use strict;
use warnings;

use lib qw(t benchmark);

use Benchmark ();
use DBI;
use InclineTest;
use pivot;

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
setup_db($db->dsn);
system(
    qw(src/incline),
    "--rdbms=$ENV{TEST_DBMS}",
    qw(--source=benchmark/pivot-standalone-def.json --port=19010 --database=test
       create-trigger),
) == 0 or die "src/incline failed: $?";

my %bench;
# benchmark
for (my $i = 0; $i < 3; $i++) {
    # direct
    for my $rows_per_stmt (1, 10, 100) {
        print STDERR ".";
        push_bench(
            ($bench{"insert ($rows_per_stmt rows)"} ||= {}),
            'direct',
            sub { do_insert($db->dsn, 'd', $rows_per_stmt) },
        );
        print STDERR ".";
        push_bench(
            ($bench{"delete ($rows_per_stmt rows)"} ||= {}),
            'direct',
            sub { do_delete($db->dsn, 'd', $rows_per_stmt) },
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
            sub { do_insert($db->dsn, 'i_s', $rows_per_stmt) },
        );
        DBI->connect($db->dsn)->selectrow_arrayref(
            'SELECT COUNT(*) FROM i_d',
        )->[0] == $pivot::ROWS
            or die "logic flaw";
        print STDERR ".";
        push_bench(
            ($bench{"delete ($rows_per_stmt rows)"} ||= {}),
            'incline',
            sub { do_delete($db->dsn, 'i_s', $rows_per_stmt) },
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
