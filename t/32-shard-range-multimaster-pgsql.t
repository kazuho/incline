use strict;
use warnings;

use List::MoreUtils qw(apply);

$ENV{TEST_DBMS} = 'postgresql';

require(apply { s/\-pgsql.t$/\.pl/ } $0);
