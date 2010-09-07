#!/usr/bin/perl

# Copyright (C) 2010   Keith Amling, keith.amling@gmail.com
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

$| = 1;

use warnings;
use strict;

my @block;
my @keep;
my $raw = 0;
my $type = "Z";
while(@ARGV)
{
   my $arg = shift;
   if(!$raw)
   {
      if($arg eq "--")
      {
         $raw = 1;
         next;
      }
      if($arg eq "-k")
      {
         $type = "K";
         next;
      }
      if($arg eq "-b")
      {
         $type = "B";
         next;
      }
   }
   if($type eq "K")
   {
      push @keep, $arg;
      $type = "Z";
   }
   elsif($type eq "B")
   {
      push @block, $arg;
      $type = "Z";
   }
   elsif($type eq "Z")
   {
      if(@block)
      {
         push @keep, $arg;
      }
      else
      {
         push @block, $arg;
      }
   }
}
die "Args!\n" if(!@block);
my %block;
@ARGV = @block;
while(<>)
{
   chomp;
   $block{$_} = "";
}
@ARGV = @keep;
while(<>)
{
   chomp;
   if(!exists($block{$_}))
   {
      print $_ . "\n";
   }
}
