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

use strict;
use warnings;

my $x =
{
   "good" => [[1, '!"#$%&' . "'" . '()*+,-./:;<=>?@[\]^_`{|}~'], [1, '0123456789'], [1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'], [5, 'abcdefghijklmnopqrstuvwxyz']],
   "8all" => [[8, '!"#$%&' . "'" . '()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~']],
   "30an" => [[30, '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz']],
   "8an"  => [[8, '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz']],
};

print join("", map { $_->[0] } sort { $a->[1] <=> $b->[1] } map { [$_, rand()] } map { my $s = $_->[1]; map { substr($s, int(rand() * length($s)), 1) } (1..$_->[0]) } @{$x->{shift || "30an"}}) . "\n";
