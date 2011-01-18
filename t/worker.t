use t::Utils;
use Test::More;
use Jonk;

my $dbh = t::Utils->setup;

subtest 'grab_job' => sub {
    my $client = Jonk->new($dbh, {});

    my $job_id = $client->insert('MyWorker', 'arg');
    ok $job_id;

    my $job = $client->find_job(+{functions => [qw/MyWorker/]});
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';
    is $job->retry_cnt, 0;
    is $job->run_after, 0;
    is $job->priority, 0;

    ok not $client->errstr;

    $job->completed;

    ok not $client->find_job(+{functions => [qw/MyWorker/]});
};

done_testing;
__END__
subtest 'grab_job / no job' => sub {
    my $client = Jonk->new($dbh, {functions => [qw/MyWorker/]});
    my $job = $client->grab_job;
    ok not $job;
};

subtest 'grab_job / specific job_id' => sub {
    my $client = Jonk->new($dbh);

    my $job_id = $client->insert('MyWorker', 'grab_job');
    ok $job_id;

    my $job = $client->grab_job($job_id);
    is $job->arg, 'grab_job';
    is $job->func, 'MyWorker';
    $job->completed;
    ok not $client->grab_job;
};

t::Utils->cleanup($dbh);

subtest 'grab_job / flexible job table name' => sub {
    my $dbh = t::Utils->setup("my_job");
    my $client = Jonk->new($dbh, { table_name => "my_job" });

    my $job_id = $client->insert('MyWorker', 'arg');
    ok $job_id;

    my $jonk = Jonk->new($dbh, { table_name => "my_job", functions => [qw/MyWorker/]});
    my $job = $jonk->grab_job;
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';
    ok not $jonk->errstr;
    $job->completed;
    ok not $client->grab_job;

    t::Utils->cleanup($dbh, "my_job");
};

done_testing;

