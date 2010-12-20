use strict;
use warnings;
use t::Utils;
use Test::More;
use Jonk;

my $dbh = t::Utils->setup;

subtest 'lookup' => sub {
    my $client = Jonk->new($dbh, {functions => [qw/MyWorker/]});

    my $job_id = $client->enqueue('MyWorker', 'arg');
    ok $job_id;

    my $job = $client->lookup;
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';
    ok not $client->errstr;
    $job->dequeue;
    ok not $client->lookup;
};

subtest 'lookup / no job' => sub {
    my $client = Jonk->new($dbh, {functions => [qw/MyWorker/]});
    my $job = $client->lookup;
    ok not $job;
};

subtest 'lookup / lookup specific job_id' => sub {
    my $client = Jonk->new($dbh);

    my $job_id = $client->enqueue('MyWorker', 'lookup_job');
    ok $job_id;

    my $job = $client->lookup($job_id);
    is $job->arg, 'lookup_job';
    is $job->func, 'MyWorker';
    $job->dequeue;
    ok not $client->lookup;
};

t::Utils->cleanup($dbh);

subtest 'lookup / flexible job table name' => sub {
    my $dbh = t::Utils->setup("my_job");
    my $client = Jonk->new($dbh, { table_name => "my_job" });

    my $job_id = $client->enqueue('MyWorker', 'arg');
    ok $job_id;

    my $jonk = Jonk->new($dbh, { table_name => "my_job", functions => [qw/MyWorker/]});
    my $job = $jonk->lookup;
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';
    ok not $jonk->errstr;
    $job->dequeue;
    ok not $client->lookup;

    t::Utils->cleanup($dbh, "my_job");
};

done_testing;

