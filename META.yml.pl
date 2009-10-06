#! /usr/bin/perl

use strict;
use warnings;

use File::Slurp;

print <<"EOT";
--- #YAML:1.0
name: Module-Build
abstract: Build and install Perl modules
version: @{[ do {
    my $s = `src/incline --version`;
    chomp $s;
    $s;
}]}
author:
@{[
    join "\n", map {
        "  - $_"
    } split /\n/, read_file('AUTHORS')
]}
license: bsd
distribution_type: script
requires:
  File::Slurp
  perl: 5.008
build_requires:
test_requires:
  DBI: 0
  List::MoreUtils: 0
  Scope::Guard: 0
  Test::mysqld: 0
  Test::postgresqld: 0
resources:
  license: http://dev.perl.org/licenses/
meta-spec:
  version: 1.4
  url: http://module-build.sourceforge.net/META-spec-v1.4.html
generated_by: META.yml.pl
EOT
