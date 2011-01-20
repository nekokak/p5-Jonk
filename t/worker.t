use t::Utils;
use Test::More;
use Jonk;

my $dbh = t::Utils->setup;

subtest 'find_job' => sub {
    my $client = Jonk->new($dbh, {functions => {MyWorker => {}}});

    my $job_id = $client->insert('MyWorker', 'arg');
    ok $job_id;

    my $job = $client->find_job();
    ok $job;
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';
    is $job->retry_cnt, 0;
    is $job->run_after, 0;
    is $job->priority, 0;

    ok not $client->errstr;

    $job->completed;

    ok not $client->find_job();
};

subtest 'find_job / with priority' => sub {
    my $client = Jonk->new($dbh, {functions => {MyWorker => {}}});

    $client->insert('MyWorker', 'arg_10', {priority => 10});
    $client->insert('MyWorker', 'arg_30', {priority => 30});
    $client->insert('MyWorker', 'arg_20', {priority => 20});

    my $job = $client->find_job();
    is $job->arg, 'arg_30';
    is $job->priority, 30;

    ok not $client->errstr;

    $job->completed;

    $job = $client->find_job();

    is $job->arg, 'arg_20';
    is $job->priority, 20;

    ok not $client->errstr;

    $job->completed;

    $job = $client->find_job();

    is $job->arg, 'arg_10';
    is $job->priority, 10;

    ok not $client->errstr;

    $job->completed;
};

subtest 'find_job / with run_after' => sub {
    my $client = Jonk->new($dbh, {functions => {MyWorker => {}}});

    my $time = time() + 2;
    $client->insert('MyWorker', 'arg', {run_after => $time});

    ok not $client->find_job();

    sleep 2;

    my $job = $client->find_job();
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';
    is $job->retry_cnt, 0;
    is $job->run_after, $time;
    is $job->priority, 0;

    ok not $client->errstr;

    $job->completed;

    ok not $client->find_job();
};

subtest 'find_job / with grabbed_until' => sub {

    {
        my $client = Jonk->new($dbh, {functions => {MyWorker => {}}, default_grab_for => 2});
        $client->insert('MyWorker', 'arg');

        my $job = $client->find_job();
        is $job->arg, 'arg';
        is $job->func, 'MyWorker';
        is $job->retry_cnt, 0;
        is $job->run_after, 0;
        is $job->priority, 0;

        ok not $client->errstr;

        ok not $client->find_job();

        sleep 2;

        my $re_grabbed_job = $client->find_job();
        is $re_grabbed_job->arg, 'arg';
        is $re_grabbed_job->func, 'MyWorker';
        is $re_grabbed_job->retry_cnt, 0;
        is $re_grabbed_job->run_after, 0;
        is $re_grabbed_job->priority, 0;

        $re_grabbed_job->completed;

        ok not $client->find_job();
    }

    {
        my $client = Jonk->new($dbh, {functions => {MyWorker => {grab_for => 5}}, default_grab_for => 2});
        $client->insert('MyWorker', 'arg');

        my $job = $client->find_job();
        is $job->arg, 'arg';
        is $job->func, 'MyWorker';
        is $job->retry_cnt, 0;
        is $job->run_after, 0;
        is $job->priority, 0;

        ok not $client->errstr;

        ok not $client->find_job();
        sleep 2;

        ok not $client->find_job();
        sleep 3;

        my $re_grabbed_job = $client->find_job();
        is $re_grabbed_job->arg, 'arg';
        is $re_grabbed_job->func, 'MyWorker';
        is $re_grabbed_job->retry_cnt, 0;
        is $re_grabbed_job->run_after, 0;
        is $re_grabbed_job->priority, 0;

        $re_grabbed_job->completed;

        ok not $client->find_job();
    }
};

subtest 'find_job / without functions' => sub {
    my $client = Jonk->new($dbh);

    $client->insert('MyWorker', 'arg');

    eval { $client->find_job };
    like $@, qr/missin find_job functions. at /;
};

subtest 'lookup_job' => sub {
    my $client = Jonk->new($dbh);

    my $job_id = $client->insert('MyWorker', 'arg');
    ok $job_id;

    my $job = $client->lookup_job($job_id);
    is $job->func, 'MyWorker';
    is $job->arg, 'arg';

    ok not $client->errstr;

    $job->completed;

    ok not $client->lookup_job($job_id);
};

t::Utils->cleanup($dbh);

subtest 'find_job / flexible job table name' => sub {
    my $dbh = t::Utils->setup("my_job");
    my $client = Jonk->new($dbh, { table_name => 'my_job', functions => {MyWorker => {}}});

    my $job_id = $client->insert('MyWorker', 'arg');
    ok $job_id;

    my $job = $client->find_job();
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';

    ok not $client->errstr;

    $job->completed;

    ok not $client->find_job;

    t::Utils->cleanup($dbh, "my_job");
};

subtest 'lookup_job / flexible job table name' => sub {
    my $dbh = t::Utils->setup("my_job");
    my $client = Jonk->new($dbh, { table_name => 'my_job', functions => {MyWorker => {}}});

    my $job_id = $client->insert('MyWorker', 'arg');
    ok $job_id;

    my $job = $client->lookup_job($job_id);
    is $job->arg, 'arg';
    is $job->func, 'MyWorker';

    ok not $client->errstr;

    $job->completed;

    ok not $client->lookup_job($job_id);

    t::Utils->cleanup($dbh, "my_job");
};
done_testing;

