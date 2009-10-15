#! /usr/bin/perl

use strict;
use warnings;

use lib qw(t);

use Benchmark ();
use DBI;
use InclineTest;

my $ROWS = 100_000;
my $ROWS_PER_STMT = 100;

# setup
Benchmark::enablecache();
my $db = init_db(
    mysqld => {
        my_cnf => {
            'bind-address'                 => '127.0.0.1',
            port                           => 19010,
            'default-storage-engine'       => 'INNODB',
            innodb_buffer_pool_size        => '128M',
            innodb_flush_log_at_trx_commit => 0,
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
$dbh->do(adjust_ddl('CREATE TABLE d (x SERIAL,y INT NOT NULL,PRIMARY KEY (x,y),KEY y_x (y,x))'))
    or die $dbh->errstr;
$dbh->do(adjust_ddl('CREATE TABLE i_s (x SERIAL,y INT NOT NULL,PRIMARY KEY (x,y))'))
    or die $dbh->errstr;
$dbh->do(adjust_ddl('CREATE TABLE i_d (y INT NOT NULL,x INT NOT NULL,PRIMARY KEY (y,x))'))
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

my (%insert_bench, %delete_bench);
# benchmark
for (my $i = 0; $i < 4; $i++) {
    # direct
    print STDERR ".";
    push_bench(\%insert_bench, 'direct', sub { do_insert('d') });
    print STDERR ".";
    push_bench(\%delete_bench, 'direct', sub { do_delete('d') });
    # incline
    print STDERR ".";
    push_bench(\%insert_bench, 'incline', sub { do_insert('i_s') });
    print STDERR ".";
    push_bench(\%delete_bench, 'incline', sub { do_delete('i_s') });
}
print STDERR "\n\n";

# print
print "insert speed:\n";
Benchmark::cmpthese(\%insert_bench);
print "\ndelete speed:\n";
Benchmark::cmpthese(\%delete_bench);
print "\n";
