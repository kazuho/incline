package Test::mysqld;

use strict;
use warnings;

use Class::Accessor::Lite;
use Cwd;
use File::Remove;
use POSIX qw(SIGTERM WNOHANG);
use Time::HiRes qw(sleep);

my %Defaults = (
    auto_start       => 2,
    base_dir         => undef,
    my_cnf           => {},
    mysql_install_db => undef,
    mysqld           => undef,
);

Class::Accessor::Lite->mk_accessors(keys %Defaults);

sub new {
    my $klass = shift;
    my $self = bless {
        %Defaults,
        @_ == 1 ? %{$_[0]} : @_
    }, $klass;
    $self->my_cnf({
        'bind-address' => '127.0.0.1',
        %{$self->my_cnf},
    });
    die "no ''base_dir''"
        unless $self->base_dir;
    $self->base_dir(cwd . '/' . $self->base_dir)
        if $self->base_dir !~ m|^/|;
    $self->my_cnf->{socket} ||= $self->base_dir . "/tmp/mysql.sock";
    $self->my_cnf->{datadir} ||= $self->base_dir . "/var";
    $self->my_cnf->{'pid-file'} ||= $self->base_dir . "/tmp/mysqld.pid";
    $self->mysql_install_db(_find_program(qw/mysql_install_db bin scripts/))
        unless $self->mysql_install_db;
    $self->mysqld(_find_program(qw/mysqld bin libexec/))
        unless $self->mysqld;
    die 'mysqld is already running (' . $self->my_cnf->{'pid-file'} . ')'
        if -e $self->my_cnf->{'pid-file'};
    if ($self->auto_start) {
        $self->setup
            if $self->auto_start >= 2;
        $self->start;
    }
    $self;
}

sub DESTROY {
    my $self = shift;
    $self->stop
        if $self->is_running;
}

sub is_running {
    my $self = shift;
    -e $self->my_cnf->{'pid-file'};
}

sub start {
    my $self = shift;
    return
        if $self->is_running;
    open my $logfh, '>>', $self->base_dir . '/tmp/mysqld.log'
        or die 'failed to create log file:' . $self->base_dir
            . "/tmp/mysqld.log:$!";
    my $pid = fork;
    die "fork(2) failed:$!"
        unless defined $pid;
    if ($pid == 0) {
        open STDOUT, '>&', $logfh
            or die "dup(2) failed:$!";
        open STDERR, '>&', $logfh
            or die "dup(2) failed:$!";
        exec(
            $self->mysqld,
            '--defaults-file=' . $self->base_dir . '/etc/my.cnf',
        );
        die "failed to launch mysqld:$?";
    } else {
        close $logfh;
        while (! -e $self->my_cnf->{'pid-file'}) {
            if (waitpid($pid, WNOHANG) > 0) {
                die "failed to launch mysqld, see tmp/mysqld.log for details.";
            }
            sleep 0.1;
        }
    }
}

sub stop {
    my $self = shift;
    if ($self->is_running) {
        open my $fh, '<', $self->my_cnf->{'pid-file'}
            or die 'could not open ' . $self->my_cnf->{'pid-file'} . ":$!";
        my $pid = <$fh>;
        chomp $pid;
        close $fh;
        kill SIGTERM, $pid;
        while ($self->is_running) {
            sleep 0.1;
        }
    }
}

sub setup {
    my $self = shift;
    # (re)create directory structure
    File::Remove::remove(\1, glob $self->base_dir . "/*");
    mkdir $self->base_dir;
    for my $subdir (qw/etc var tmp/) {
        mkdir $self->base_dir . "/$subdir"
            or die "failed to create dir:" . $self->base_dir . "/$subdir:$!";
    }
    # my.cnf
    open my $fh, '>', $self->base_dir . '/etc/my.cnf'
        or die "failed to create file:" . $self->base_dir . "/etc/my.cnf:$!";
    print $fh "[mysqld]\n";
    print $fh map {
        "$_=" . $self->my_cnf->{$_} . "\n"
    } sort keys %{$self->my_cnf};
    close $fh;
    # mysql_install_db
    open(
        $fh,
        '-|',
        $self->mysql_install_db . " --defaults-file='" . $self->base_dir
            . "/etc/my.cnf' 2>&1",
    ) or die "failed to spawn mysql_install_db:$!";
    my $output = do { undef $/; join "", join "\n", <$fh> };
    close $fh
        or die "mysql_install_db failed:\n$output\n";
}

sub _find_program {
    my ($prog, @subdirs) = @_;
    my $path = _get_path_of($prog);
    return $path
        if $path;
    for my $subdir (@subdirs) {
        if ($path = _get_path_of('mysql')
                and $path =~ s|/bin/mysql$|/$subdir/$prog|
                    and -x $path) {
            return $path;
        }
    }
    die "could not find $prog";
}

sub _get_path_of {
    my $prog = shift;
    my $path = `which $prog 2> /dev/null`;
    chomp $path
        if $path;
    $path;
}

1;
