use strict;
use warnings;
use t::Utils;
use Test::More;
use DBI;
use Jonk::Client;
use Jonk::Worker;

my $mysqld = t::Utils->setup;
my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'));

subtest 'enqueue' => sub {
    my $jonk = Jonk::Client->new($dbh);

    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
    my $time = sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year + 1900, $mon + 1, $mday, $hour, $min, $sec);

    my $job_id = $jonk->enqueue('MyWorker' => +{ arg => 'arg', time => $time });
    ok $job_id;

    my $sth = $dbh->prepare('SELECT * FROM job WHERE id = ?');
    $sth->execute($job_id);
    my $row = $sth->fetchrow_hashref;

    is $row->{arg}, 'arg';
    is $row->{func}, 'MyWorker';
    is $row->{enqueue_time}, $time;

    done_testing;
};

subtest 'dequeue' => sub {
    my $jonk = Jonk::Worker->new($dbh, {functions => [qw/MyWorker/]});
    my $job = $jonk->dequeue();
    is $job->{arg}, 'arg';
    is $job->{func}, 'MyWorker';

    done_testing;
};

subtest 'dequeue / no job' => sub {
    my $jonk = Jonk::Worker->new($dbh, {functions => [qw/MyWorker/]});
    my $job = $jonk->dequeue();
    ok not $job;
    done_testing;
};

t::Utils->cleanup($mysqld);

done_testing;

