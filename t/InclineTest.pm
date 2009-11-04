package InclineTest;

use strict;
use warnings;

use Benchmark qw(:hireswallclock);
use DBI;
use Exporter qw(import);
use Test::More;
use Test::mysqld;
use Test::postgresql;

our @EXPORT = qw(init_db adjust_ddl do_parallel push_bench exec_cmd);

sub init_db {
    my %opts = @_;
    # start server
    my $instance = "Test::$ENV{TEST_DBMS}"->new($opts{$ENV{TEST_DBMS}})
        or do {
            no strict 'refs';
            plan skip_all => ${"Test::".$ENV{TEST_DBMS}."::errstr"};
        };
    if ($ENV{TEST_DBMS} eq 'postgresql') {
        my $dbh = DBI->connect($instance->dsn)
            or die $DBI::errstr;
        $dbh->do('CREATE ROLE root SUPERUSER LOGIN')
            or die $dbh->errstr;
        $dbh->do(q{CREATE LANGUAGE 'plpgsql'})
            or die $dbh->errstr;
    }
    $instance;
}

sub adjust_ddl {
    my $ddl = shift;
    if ($ENV{TEST_DBMS} eq 'mysqld') {
        $ddl =~ s/(\s+)SERIAL(\W?)/$1INT UNSIGNED NOT NULL AUTO_INCREMENT$2/ig;
    }
    $ddl;
}

sub do_parallel {
    my ($num_workers, $code) = @_;
    my %pids;
    for (my $i = 0; $i < $num_workers; $i++) {
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

sub push_bench {
    my ($bench, $n, $c) = @_;
    my $t = Benchmark::runloop(1, $c, $n);
    # use time spend _outside_ this process
    $t->[1] = $t->[3] = $t->[0] - $t->[1];
    $t->[2] = $t->[4] = 0;
    $bench->{$n} = $bench->{$n} ? Benchmark::timesum($bench->{$n}, $t) : $t;
}

sub exec_cmd {
    my @args = @_;
    if ($ENV{CMD_PREFIX}) {
        if ($ENV{CMD_PREFIX} eq 'gdb') {
            print STDERR "*** running: ", join(' ', @args), "\n";
            exec('gdb', shift @args);
        } else {
            exec((split /\s+/, $ENV{CMD_PREFIX}), @args);
        }
    } else {
        exec @args;
    }
}

1;
