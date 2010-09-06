#!/usr/bin/perl

$| = 1;

use strict;
use warnings;

use Data::Dumper;
use Amling::ForkManager;

my @jobs = map { "$_-1" } (10, 3, 7, 1);

my $fm = Amling::ForkManager->new('limit' => 2, 'on_result' => \&on_result);

for my $j (@jobs)
{
    $fm->add_job($j, \&child);
}

print Dumper($fm->wait_all());

sub child
{
    my $j = shift;

    my ($sleep, $nbr) = split(/-/, $j);

    print "Starting sleep $sleep #$nbr...\n";
    sleep $sleep;
    print "Finished sleep $sleep #$nbr...\n";
}

sub on_result
{
    my $fm = shift;
    my $id = shift;
    my $result = shift;

    my ($sleep, $nbr) = split(/-/, $id);

    ++$nbr;

    $fm->add_job("$sleep-$nbr", \&child);

    return $result;
}
