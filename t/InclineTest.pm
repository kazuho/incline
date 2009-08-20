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
        or do {
            no strict 'refs';
            plan skip_all => ${"Test::".$ENV{TEST_DBMS}."::errstr"};
        };
    if ($ENV{TEST_DBMS} eq 'postgresql') {
        my $dbh = DBI->connect(
            'DBI:Pg:dbname=template1;user=postgres;port=' . $instance->port,
        ) or die $DBI::errstr;
        $dbh->do('CREATE ROLE root SUPERUSER LOGIN')
            or die $dbh->errstr;
        $dbh->do('CREATE DATABASE test')
            or die $dbh->errstr;
        $dbh = DBI->connect(
            'DBI:Pg:dbname=test;user=root;port=' . $instance->port,
        ) or die $DBI::errstr;
        $dbh->do(q{CREATE LANGUAGE 'plpgsql'})
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

sub adjust_ddl {
    my ($klass, $ddl) = @_;
    if ($ENV{TEST_DBMS} eq 'mysqld') {
        $ddl =~ s/(\s+)SERIAL(\W?)/$1INT UNSIGNED NOT NULL AUTO_INCREMENT$2/ig;
    }
    $ddl;
}

1;
