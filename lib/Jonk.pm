package Jonk;
use strict;
use warnings;
use Jonk::Job;
use Try::Tiny;
use Carp ();

our $VERSION = '0.10_02';

sub new {
    my ($class, $dbh, $opts) = @_;

    unless ($dbh) {
        Carp::croak('missing job queue database handle.');
    }

    my $functions  = _parse_functions($opts);
    my $table_name = $opts->{table_name} || 'job';
    my $driver     = _verify_driver($dbh);

    bless {
        dbh           => $dbh,
        table_name    => $table_name,
        functions     => $functions,
        driver        => $driver,
        has_func      => scalar(keys %{$functions}) ? 1 : 0,

        _errstr       => undef,

        insert_query => sprintf(
            'INSERT INTO %s (func, arg, enqueue_time, grabbed_until, run_after, retry_cnt, priority) VALUES (?,?,?,0,?,0,?)'
            ,$table_name
        ),

        grab_query => sprintf('UPDATE %s SET grabbed_until = ? WHERE id = ? AND grabbed_until = ?', $table_name),

        lookup_job_query => sprintf('SELECT * FROM %s WHERE id = ? AND grabbed_until <= ? AND run_after <= ?', $table_name),

        find_job_query => sprintf(
            'SELECT * FROM %s WHERE func IN (%s) AND grabbed_until <= ? AND run_after <= ? ORDER BY priority DESC LIMIT %s',
            $table_name, (join(', ', map { "'$_'" } keys %{$functions})), ($opts->{job_find_size} || 50)
        ),

        delete_query => sprintf('DELETE FROM %s WHERE id = ?', $table_name),

        failed_query => sprintf('UPDATE %s SET retry_cnt = retry_cnt + 1, run_after = ?, grabbed_until = 0, priority = ? WHERE id = ?', $table_name),

        unixtime_query => _settled_unixtime_query($driver),

    }, $class;
}

sub _parse_functions {
    my $opts = shift;

    my $functions = $opts->{functions} || [];
    my $default_grab_for = $opts->{default_grab_for} || (60*60);

    my $funcs = +{};
    for (my $i = 0; $i < @{$functions}; $i++) {
        my $func = $functions->[$i];

        my $value;
        if    (not defined $functions->[$i+1]) {$i++                       }
        elsif (ref $functions->[$i+1])         {$value = $functions->[++$i]}

        $value->{grab_for}     ||= $default_grab_for;
        $value->{serializer}   ||= ($opts->{default_serializer}   || sub {$_[0]});
        $value->{deserializer} ||= ($opts->{default_deserializer} || sub {$_[0]});

        $funcs->{$func} = $value;
    }
    $funcs;
}

sub _verify_driver {
    my $dbh = shift;
    my $driver = $dbh->{Driver}{Name};
    $driver =~ /(mysql|SQLite|Pg)/ ? $driver : Carp::croak('Jonk support only mysql,SQLite,Pg');
}

sub _settled_unixtime_query {
    my $driver = shift;

    if ($driver eq 'Pg') {
        return "SELECT TRUNC(EXTRACT('epoch' from NOW()))";
    } elsif ($driver eq 'mysql') {
        return 'SELECT UNIX_TIMESTAMP()';
    }
}

sub errstr {$_[0]->{_errstr}}

sub insert {
    my ($self, $func, $arg, $opt) = @_;

    my $job_id;
    try {
        $self->{_errstr} = undef;
        local $self->{dbh}->{RaiseError} = 1;
        local $self->{dbh}->{PrintError} = 0;

        my $serializer = $self->{functions}->{$func}->{serializer} ||= sub {$_[0]};
        my $sth = $self->{dbh}->prepare_cached($self->{insert_query});
        $sth->bind_param(1, $func);
        $sth->bind_param(2, $serializer->($arg), _bind_param_attr($self->{driver}));
        $sth->bind_param(3, time());
        $sth->bind_param(4, $opt->{run_after}||0);
        $sth->bind_param(5, $opt->{priority} ||0);
        $sth->execute();

        $job_id = $self->{dbh}->last_insert_id("","",$self->{table_name},"");
        $sth->finish;
    } catch {
        $self->{_errstr} = "can't insert for job queue database: $_"
    };

    $job_id;
}

sub _bind_param_attr {
    my $driver = shift;

    if ( $driver eq 'Pg' ) {
        return { pg_type => DBD::Pg::PG_BYTEA() };
    } elsif ( $driver eq 'SQLite' ) {
        return DBI::SQL_BLOB();
    }
    return;
}

sub _server_unixitime {
    my $self = shift;
    return time() if $self->{driver} eq 'SQLite';
    $self->{dbh}->selectrow_array($self->{unixtime_query});
}

sub _grab_job {
    my ($self, $callback, $opt) = @_;

    my $job;
    try {
        $self->{_errstr} = undef;
        local $self->{dbh}->{RaiseError} = 1;
        local $self->{dbh}->{PrintError} = 0;

        my $time = $self->_server_unixitime;
        my $sth = $callback->($time);

        while (my $row = $sth->fetchrow_hashref) {
            $job = $self->_grab_a_job($row, $time, $opt);
            last if $job;
        }

        $sth->finish;
    } catch {
        $self->{_errstr} = "can't grab job from job queue database: $_";
    };

    $job;

}

sub _grab_a_job {
    my ($self, $row, $time) = @_;

    my $sth = $self->{dbh}->prepare_cached($self->{grab_query});
    $sth->execute(
        ($time + ($self->{functions}->{$row->{func}}->{grab_for})),
        $row->{id},
        $row->{grabbed_until}
    );
    my $grabbed = $sth->rows;
    $sth->finish;
    $grabbed ? Jonk::Job->new($self => $row) : undef;
}

sub lookup_job {
    my ($self, $job_id) = @_;

    $self->_grab_job(
        sub {
            my $time = shift;
            my $sth = $self->{dbh}->prepare_cached($self->{lookup_job_query});
            $sth->execute($job_id, $time, $time);
            $sth;
        }
    );
}

sub find_job {
    my $self = shift;

    unless ($self->{has_func}) {
        Carp::croak('missin find_job functions.');
    }

    $self->_grab_job(
        sub {
            my $time = shift;
            my $sth = $self->{dbh}->prepare_cached($self->{find_job_query});
            $sth->execute($time, $time);
            $sth;
        }
    );
}

sub _delete {
    my ($self, $job_id) = @_;

    try {
        my $sth = $self->{dbh}->prepare_cached($self->{delete_query});
        $sth->execute($job_id);
        $sth->finish;
        return $sth->rows;
    } catch {
        $self->{_errstr} = "can't dequeue job from job queue database: $_";
        return;
    };
}

sub _failed {
    my ($self, $job_id, $opt) = @_;

    my $retry_delay = $self->_server_unixitime + (defined($opt->{retry_delay}) ? $opt->{retry_delay} : 60);
    my $priority    = (defined($opt->{priority}) ? $opt->{priority} : 0);

    try {
        my $sth = $self->{dbh}->prepare_cached($self->{failed_query});
        $sth->execute($retry_delay, $priority, $job_id);
        $sth->finish;
        return $sth->rows;
    } catch {
    warn 'ababaaaba';
        $self->{_errstr} = "can't update job from job queue database: $_";
        return;
    };
}

1;

__END__

=head1 NAME

Jonk - simple job tank manager.

=head1 SYNOPSIS

    use DBI; 
    use Jonk;
    my $dbh = DBI->connect(...);
    my $jonk = Jonk->new($dbh, {functions => [qw/MyWorker/]});
    # insert job
    {
        $jonk->insert('MyWorker', 'arg');
    }

    # execute job
    {
        my $job = $jonk->find_job;
        print $job->func; # MyWorker
        print $job->arg;  # arg
        $job->completed;
    }

=head1 DESCRIPTION

Jonk is simple job queue manager system

Job is saved and taken out. Besides, nothing is done.

You may use Jonk to make original Job Queuing System.

=head1 METHODS

=head2 my $jonk = Jonk::Worker->new($dbh, [\%options]);

Creates a new Jonk object, and returns the object.

$options is an optional settings.

=over 4

=item * $dbh

$dbh is database handle.

=item * $options->{functions}

Key word of job which this Jonk instance looks for.

=over 4

=item * $options->{functions} = [qw/worker_key worker_key2/]

can set *worker_key* at arrayref.

=item * $options->{functions} = ['worker_key' => {grab_for => 5}],

can set worker_key's grab_for setting by hash-ref.

=item * $options->{functions} = ['worker_key' => {serializer => \&serialize_code, deserializer => \&deserialize_code}],

can set worker_key's (de)serializer code setting by hash-ref.

=item * $options->{functions} = ['worker_key' => {serializer => \&serialize_code, deserializer => \&deserialize_code}, 'worker_key2'],

can mix worker settings.

=back

=item * $options->{table_name}

specific job table name.

Default job table name is `job`.

=item * $options->{job_find_size}

specific lookup job record size.

Default 50.

=item * $options->{default_serializer}

global serializer setting.

=item * $options->{default_deserializer}

global deserializer setting.

=item * $options->{default_grab_for}

global grab_for setting.

=back

=head2 my $job_id = $jonk->insert($func, $arg);

enqueue a job to a database.
returns job.id.

=over 4

=item * $func

specific your worker funcname.

=item * $arg

job argument data.

=back

=head2 my $job = $jonk->lookup_job($job_id);

lookup a job from a database.

returns Jonk::Job object.

=over 4

=item * $job_id

lookup specific $job_id's job.

=back

=head2 my $job = $jonk->find_job();

get job from database by sorted priority descending order.

=head2 $jonk->errstr;

get most recent error infomation.

=head1 ERROR HANDLING

  my $job = $jonk->lookup;
  if ($jonk->errstr) {
      die $jonk->errstr;
  }

=head1 SCHEMA

=head2 MySQL

    CREATE TABLE job (
        id            int(10) UNSIGNED NOT NULL auto_increment,
        func          varchar(255)     NOT NULL,
        arg           MEDIUMBLOB,
        enqueue_time  INTEGER UNSIGNED,
        grabbed_until int(10) UNSIGNED NOT NULL,
        run_after     int(10) UNSIGNED NOT NULL DEFAULT 0,
        retry_cnt     int(10) UNSIGNED NOT NULL DEFAULT 0,
        priority      int(10) UNSIGNED NOT NULL DEFAULT 0,
        primary key ( id )
    ) ENGINE=InnoDB

=head2 SQLite

    CREATE TABLE job (
        id            INTEGER PRIMARY KEY ,
        func          text,
        arg           text,
        enqueue_time  INTEGER UNSIGNED,
        grabbed_until INTEGER UNSIGNED NOT NULL,
        run_after     INTEGER UNSIGNED NOT NULL DEFAULT 0,
        retry_cnt     INTEGER UNSIGNED NOT NULL DEFAULT 0,
        priority      INTEGER UNSIGNED NOT NULL DEFAULT 0
    )

=head2 PostgreSQL

    CREATE TABLE job (
        id            SERIAL PRIMARY KEY,
        func          TEXT NOT NULL,
        arg           BYTEA,
        enqueue_time  INTEGER,
        grabbed_until INTEGER NOT NULL,
        run_after     INTEGER NOT NULL DEFAULT 0,
        retry_cnt     INTEGER NOT NULL DEFAULT 0,
        priority      INTEGER NOT NULL DEFAULT 0
    )

=head1 SEE ALSO

L<Qudo>

L<TheSchwartz>

=head1 SUPPORT

  irc: #jonk@irc.perl.org

=head1 REPOSITORY

  git clone git://github.com/nekokak/p5-Jonk.git

=head1 CONTRIBUTORS

tokuhirom

kan_fushihara

fujiwara

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

