package Test::mysqld;

use strict;
use warnings;

use Class::Accessor::Lite;
use Cwd;
use File::Remove;
use Time::HiRes qw(sleep);

my %Defaults = (
    auto_start       => 1,
    base_dir         => undef,
    my_cnf           => {},
    mysql_install_db => undef,
    mysqld           => undef,
    skip_setup       => undef,
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
    $self->setup()
        if ! $self->skip_setup;
    $self->start
        if $self->auto_start;
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
    if (! $self->is_running) {
        system(
            $self->mysqld . " --defaults-file='" . $self->base_dir
                . "/etc/my.cnf' > " . $self->base_dir
                    . '/tmp/mysqld.log 2>&1 &',
        ) == 0
            or die "failed to launch mysqld:$?";
        while (! $self->is_running) {
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
        kill 15, $pid;
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
    system(
        $self->mysql_install_db . " --defaults-file='" . $self->base_dir
            . "/etc/my.cnf' > " . $self->base_dir
                . '/tmp/mysql_install_db.log 2>&1',
    ) == 0
        or die "mysql_install_db failed:$?";
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
