#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

use Data::Dumper;
use Amling::ForkManager;

my @jobs = (10, 3, 7, 1);

my $fm = Amling::ForkManager->new('limit' => 2);

for my $j (@jobs)
{
    $fm->add_job($j, \&child);
}

print Dumper($fm->wait_all());

sub child
{
    my $j = shift;

    print "Starting sleep $j...\n";
    sleep $j;
    print "Finished sleep $j...\n";

    return "Slept for $j";
}
