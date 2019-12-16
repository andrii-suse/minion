use Mojo::Base -strict;
use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use Minion;
use Mojo::IOLoop::ReadWriteProcess qw(queue process);
require Mojo::Pg;

use Data::Dumper;

my $iterations = 100;

my $pg = Mojo::Pg->new($ENV{TEST_ONLINE});
$pg->db->query('drop schema if exists minion_worker_test cascade');
$pg->db->query('create schema minion_worker_test');
my $minion = Minion->new(Pg => $ENV{TEST_ONLINE});
$minion->backend->pg->search_path(['minion_worker_test']);
 
my $q = queue;
 
$q->pool->maximum_processes(16); # Max processes in parallel
$q->queue->maximum_processes($iterations); # Max queue
my $fired;

$q->add( process sub {
    my $res = $minion->lock('test_lock', 3600, {limit => 16});
    $minion->unlock('test_lock') if $res;
} ) for 1..$iterations;

$q->once(stop => sub { $fired++; });

my $locks = $minion->backend->pg->db->query(
  "select count(*) as cnt
   from minion_worker_test.minion_locks where name='test_lock'"
)->hash->{cnt};

is($locks, 0, 'No locks should exist');

# Consume the queue
$q->consume();
my $all = $q->done;

is($fired, $iterations, 'Number of executed events matches planned');
print Dumper('Events not fired: ', $all) unless $fired == $iterations;

$locks = $minion->backend->pg->db->query(
  "select count(*) as cnt
   from minion_worker_test.minion_locks where name='test_lock'"
)->hash->{cnt};

is($locks, 0, 'No locks should remain');

$pg->db->query('drop schema minion_worker_test cascade');

done_testing();
