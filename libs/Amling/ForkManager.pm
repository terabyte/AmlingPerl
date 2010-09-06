package Amling::ForkManager;

use strict;
use warnings;

use Storable ('freeze', 'thaw');
use Time::HiRes ('time');
use POSIX (':sys_wait_h');

# TODO: allow stdout/stdin close/noclose
# TODO: error handling (on_error which can decide halt, result, etc.)

sub new
{
    my $class = shift;
    my %args = @_;

    my $on_fork = _default_arg(delete($args{"on_fork"}), sub {});
    my $on_result = _default_arg(delete($args{"on_result"}), \&_default_on_result);
    my $limit = _default_arg(delete($args{"limit"}), -1);
    my $comparator = delete($args{"comparator"});

    my $this =
    {
        'ON_FORK' => $on_fork,
        'ON_RESULT' => $on_result,
        'LIMIT' => $limit,
        'COMPARATOR' => $comparator,

        'QUEUE' => [],
        'IN_FLIGHT' => [],
        'RESULTS' => {},
        'HALTING' => 0,
    };

    bless $this, $class;
    return $this;
}

sub add_job
{
    my $this = shift;
    my $id = shift;
    my $subref = shift;
    my $args = shift || [];

    push @{$this->{'QUEUE'}}, [$id, $subref, $args];
    $this->_sort();

    $this->_maybe_spawn();
}

sub _sort
{
    my $this = shift;

    my $queue = $this->{'QUEUE'};
    my $comparator = $this->{'COMPARATOR'};

    if($comparator)
    {
        @$queue = sort { return $comparator->($a, $b) } @$queue;
    }
}

sub _maybe_spawn
{
    my $this = shift;

    my $queue = $this->{'QUEUE'};

    if(!@$queue)
    {
        return;
    }

    my $limit = $this->{'LIMIT'};
    my $in_flight = $this->{'IN_FLIGHT'};

    if($limit != -1 && @$in_flight >= $limit)
    {
        return;
    }

    my ($id, $subref, $args) = @{shift @$queue};

    my ($rh, $wh);

    pipe $rh, $wh;
    my $child = fork();

    if($child == 0)
    {
        close $rh;

        $this->child_shutdown();

#print STDERR "Starting child for $id\n";
        my $result = $subref->($id, @$args);
        my $result_storn = freeze([$result]);

        print $wh $result_storn;

        exit 0;
    }
    else
    {
        close $wh;
        push @$in_flight,
        {
            'ID' => $id,
            'CHILD_PID' => $child,
            'RH' => $rh,
            'BUFFER' => '',
        };
    }
}

sub child_shutdown
{
    my $this = shift;

    # give ON_FORK first crack at it
    $this->{'ON_FORK'}->();

    {
        for my $in_flight (@{$this->{'IN_FLIGHT'}})
        {
            close $in_flight->{'RH'};
        }
        $this->{'IN_FLIGHT'} = [];
    }

    # now we're really, really, really done
    # TODO: decide if this is a good idea or not
    %$this = ();
}

sub wait_all
{
    my $this = shift;
    my $timeout = shift;

    $this->wait_subset(undef, $timeout);

    return $this->{'RESULTS'};
}

sub wait_one
{
    my $this = shift;
    my $which = shift;
    my $timeout = shift;

    return @{$this->wait_subset([$which], $timeout)};
}

sub wait_subset
{
    my $this = shift;
    my $whiches = shift;
    my $timeout = shift;
    my $start_time = int(time() * 1000);

    while(1)
    {
        if($this->{'HALTING'})
        {
            $this->{'HALTING'} = 0;
            last;
        }

        my $still_working = $this->should_wait($whiches);
        if(!$still_working)
        {
            last;
        }

        my $rsin = '';
        for my $in_flight (@{$this->{'IN_FLIGHT'}})
        {
            vec($rsin, fileno($in_flight->{'RH'}), 1) = 1;
        }

        my $rsout = $rsin;
        my $ws = '';
        my $es = '';

        my $left = undef;
        if(defined($timeout))
        {
            my $now_time = int(time() * 1000);
            my $left = $start_time + $timeout - $now_time;
            if($left <= 0)
            {
                last;
            }
        }
        select($rsout, $ws, $es, $timeout);

        for(my $i = 0; $i < @{$this->{'IN_FLIGHT'}}; ++$i)
        {
            my $in_flight = $this->{'IN_FLIGHT'}->[$i];

            if(!vec($rsout, fileno($in_flight->{'RH'}), 1))
            {
                next;
            }

            my $buf;
            my $len = read $in_flight->{'RH'}, $buf, 1024;

            if($len)
            {
                $in_flight->{'BUFFER'} .= $buf;
            }
            else
            {
                my $result_storn = $in_flight->{'BUFFER'};
                my ($result) = @{thaw($result_storn)};

                my $munged_result = $this->{'ON_RESULT'}->($this, $in_flight->{'ID'}, $result);
                $this->{'RESULTS'}->{$in_flight->{'ID'}} = $munged_result;

                close $in_flight->{'RH'};
                waitpid $in_flight->{'CHILD_PID'}, WNOHANG;

                $this->{'IN_FLIGHT'}->[$i] = undef;
            }
        }

        $this->{'IN_FLIGHT'} = [grep { defined($_) } @{$this->{'IN_FLIGHT'}}];

        $this->_maybe_spawn();
    }

    return [map { $this->{'RESULTS'}->{$_} } @$whiches];
}

sub should_wait
{
    my $this = shift;
    my $whiches = shift;

    if(!defined($whiches))
    {
        if(@{$this->{'QUEUE'}})
        {
            return 1;
        }
        if(@{$this->{'IN_FLIGHT'}})
        {
            return 1;
        }
        return 0;
    }

    my %whiches = map { $_ => 1 } @$whiches;

    for my $queue (@{$this->{'QUEUE'}})
    {
        if($whiches{$queue->[0]})
        {
            return 1;
        }
    }

    for my $in_flight (@{$this->{'IN_FLIGHT'}})
    {
        if($whiches{$in_flight->{'ID'}})
        {
            return 1;
        }
    }

    return 0;
}

sub callback_halt
{
    my $this = shift;

    $this->{'HALTING'} = 1;
}

sub _default_on_result
{
    my $fm = shift;
    my $id = shift;
    my $result = shift;

    return $result;
}

sub _default_arg
{
    my $arg = shift;
    my $default = shift;

    if(defined($arg))
    {
        return $arg;
    }

    return $default;
}

1;
