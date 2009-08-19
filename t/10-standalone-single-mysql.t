use strict;
use warnings;

use List::MoreUtils qw(apply);

$ENV{TEST_DBMS} = 'mysqld';

require(apply { s/\.t$/\.pl/ } $0);
