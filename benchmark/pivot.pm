package pivot;

use strict;
use warnings;

use DBI;
use Exporter qw(import);
use InclineTest;

our @EXPORT = qw(setup_db do_insert do_delete);

our $ROWS = $ENV{ROWS} || 100_000;
our $NUM_WORKERS = $ENV{NUM_WORKERS} || 10;

sub setup_db {
    my $dsn = shift;
    
    my $dbh = DBI->connect($dsn, undef, undef, { AutoCommit => 1 })
        or die $DBI::errstr;
    $dbh->do('CREATE TABLE d (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y))')
	or die $dbh->errstr;
    $dbh->do('CREATE INDEX y_x ON d (y,x)')
        or die $dbh->errstr;
    $dbh->do('CREATE TABLE i_s (x INT NOT NULL,y INT NOT NULL,PRIMARY KEY (x,y))')
        or die $dbh->errstr;
    $dbh->do('CREATE TABLE i_d (y INT NOT NULL,x INT NOT NULL,PRIMARY KEY (y,x))')
        or die $dbh->errstr;
    $dbh->disconnect;
}

sub do_insert {
    my ($dsn, $table, $rows_per_stmt) = @_;
    die "condition check failed"
        unless $ROWS % ($NUM_WORKERS * $rows_per_stmt) == 0;
    do_parallel(
        $NUM_WORKERS,
        sub {
            my $base = shift(@_) * $ROWS / $NUM_WORKERS;
            my $dbh = DBI->connect($dsn, undef, undef, { AutoCommit => 1 })
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
    my ($dsn, $table, $rows_per_stmt) = @_;
    die "condition check failed"
        unless $ROWS % ($NUM_WORKERS * $rows_per_stmt) == 0;
    do_parallel(
        $NUM_WORKERS,
        sub {
            my $base = shift(@_) * $ROWS / $NUM_WORKERS;
            my $dbh = DBI->connect($dsn, undef, undef, { AutoCommit => 1 })
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

1;
