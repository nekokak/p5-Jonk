use strict;
use warnings;
use t::Utils;
use Test::More;
use DBI;
use Jonk::Client;
use Jonk::Worker;

my $mysqld = t::Utils->setup;
my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'));

subtest 'worker / flexible job table name' => sub {
    my $jonk = Jonk::Worker->new($dbh, {functions => [qw/MyWorker/]});
    is $jonk->{dequeue_query}, q{SELECT * FROM job WHERE func IN ('MyWorker') ORDER BY id LIMIT 1 FOR UPDATE};

    $jonk = Jonk::Worker->new($dbh, +{functions => [qw/MyWorker MyWorker2/]});
    is $jonk->{dequeue_query}, q{SELECT * FROM job WHERE func IN ('MyWorker', 'MyWorker2') ORDER BY id LIMIT 1 FOR UPDATE};

    $jonk = Jonk::Worker->new($dbh, +{functions => [qw/MyWorker/], table_name => 'jonk_job'});
    is $jonk->{dequeue_query}, q{SELECT * FROM jonk_job WHERE func IN ('MyWorker') ORDER BY id LIMIT 1 FOR UPDATE};

    done_testing;
};

subtest 'dequeue' => sub {
    my $client = Jonk::Client->new($dbh);

    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime(time);
    my $time = sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year + 1900, $mon + 1, $mday, $hour, $min, $sec);

    my $job_id = $client->enqueue('MyWorker' => +{ arg => 'arg', time => $time });
    ok $job_id;

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

