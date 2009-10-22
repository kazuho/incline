#! /usr/bin/perl

use strict;
use warnings;

use YAML;

my $meta = {
    name               => 'incline',
    abstract           => 'a replicator for RDB shards',
    version            => do {
        my $s = `echo VERSION | cpp -include src/incline_config.h`;
        $s =~ s/^.*\n\"([0-9_\.]+)\".*?$/$1/s
            or die "failed to obtain version number";
        $1;
    },
    author             => do {
        open my $fh, '<', 'AUTHORS'
            or die "failed to open AUTHORS:$!";
        my @authors = map {
            chomp $_;
            $_;
        } <$fh>;
        close $fh;
        \@authors;
    },
    license            => 'bsd',
    distribution_type  => 'module',
    dynamic_config     => 0,
    configure_requires => {
        perl          => '5.008',
        YAML          => 0,
    },
    requires           => {},
    build_requires     => {
        DBI                => 0,
        'List::MoreUtils'  => 0,
        'Scope::Guard'     => 0,
        'Test::mysqld'     => '0.09',
        'Test::postgresql' => '0.07',
    },
    resources          => {
        license => 'http://www.opensource.org/licenses/bsd-license.php',
    },
    no_index           => {
        directory => [ qw/example src t/ ],
        file      => [ qw/README.html/ ],
    },
    'meta-spec'        => {
        version => 1.4,
        url => 'http://module-build.sourceforge.net/META-spec-v1.4.html',
    },
    generated_by       => 'META.yml.pl',
};

if (@ARGV && $ARGV[0] eq '--fix-makefile') {

    my $prereq_expr = do {
        my %req = (
            %{$meta->{requires}},
            %{$meta->{build_requires}},
        );
        join ", ", map { "$_=>q[$req{$_}]" } sort keys %req;
    };
    print <<"EOT";
# MakeMaker Parameters:

#  PREREQ_PM => { $prereq_expr }

# --- MakeMaker post_initialize section:

EOT
    while (my $l = <STDIN>) {
        print $l;
    }

} else {

    print Dump($meta);

}

