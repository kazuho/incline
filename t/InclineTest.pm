package InclineTest;

use strict;
use warnings;

use DBI;
use Test::More;
use Test::mysqld;
use Test::postgresql;

sub create_any {
    my ($klass, %opts) = @_;
    # start server
    my $instance = "Test::$ENV{TEST_DBMS}"->new($opts{$ENV{TEST_DBMS}})
        or plan skip_all => ${"Test::".$ENV{TEST_DBMS}."::errstr"};
    if ($ENV{TEST_DBMS} eq 'postgresql') {
        my $dbh = DBI->connect(
            'DBI:Pg:dbname=template1;user=postgres;port=' . $instance->port,
        ) or die $DBI::errstr;
        $dbh->do('CREATE ROLE SUPERUSER')
            or die $dbh->errstr;
        $dbh->do('CREATE DATABASE test')
            or die $dbh->errstr;
    }
    $instance;
}

sub connect {
    my ($klass, $uri) = @_;
    my $handler;
    if ($ENV{TEST_DBMS} eq 'mysqld') {
        $handler = 'mysql';
    } elsif ($ENV{TEST_DBMS} eq 'postgresql') {
        $handler = 'Pg';
    }
    $uri =~ s/^DBI:any(\W?)/DBI:$handler$1/
        or die "unexpected template: $uri";
    my $dbh = DBI->connect($uri)
        or die $DBI::errstr;
}

1;
