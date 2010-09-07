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

my @files = @ARGV;

my %h = ();

my $first = 1;
foreach (@files)
{
   @ARGV = ($_);
   while(<>)
   {
      chomp;
      if($first)
      {
         $h{$_} = 0;
      }
      else
      {
         if(exists($h{$_}))
         {
            $h{$_} = 1;
         }
      }
   }
   if(!$first)
   {
      foreach (keys(%h))
      {
         if(!$h{$_})
         {
            delete($h{$_});
         }
         else
         {
            $h{$_} = 0;
         }
      }
   }
   else
   {
      $first = 0;
   }
}

foreach (sort(keys(%h)))
{
   print $_ . "\n";
}
