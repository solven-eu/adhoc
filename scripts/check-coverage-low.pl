#!/usr/bin/env perl
# Report production classes with < THRESHOLD% instruction coverage from JaCoCo XML reports.
# Searches recursively for target/site/jacoco/jacoco.xml files under the given roots.
# Informational only — always exits 0.
#
# Usage:  perl scripts/check-coverage-low.pl [dir ...]
#         Default search root: current directory.

use strict;
use warnings;
use File::Find;

my $THRESHOLD = 20;    # percent — anything below this is flagged

my @roots = @ARGV ? @ARGV : ('.');
my @xml_files;

find(
    sub {
        push @xml_files, $File::Find::name
            if $_ eq 'jacoco.xml'
            && $File::Find::name =~ m{/target/site/jacoco/jacoco\.xml$};
    },
    @roots
);

if ( !@xml_files ) {
    print "No jacoco.xml reports found — run tests first.\n";
    exit 0;
}

my @low_coverage;

for my $file ( sort @xml_files ) {
    open( my $fh, '<', $file ) or do { warn "Cannot open $file: $!\n"; next };

    my $current_class = '';

    while ( my $line = <$fh> ) {

        # <class name="eu/solven/adhoc/foo/Bar" sourcefilename="Bar.java">
        if ( $line =~ /<class\s+name="([^"]+)"/ ) {
            $current_class = $1;
        }

        # <counter type="INSTRUCTION" missed="15" covered="5"/>
        elsif ( $line =~ /<counter\s+type="INSTRUCTION"\s+missed="(\d+)"\s+covered="(\d+)"/ ) {
            my ( $missed, $covered ) = ( $1, $2 );
            my $total = $missed + $covered;
            if ( $total > 0 && $current_class ne '' ) {
                my $pct = 100.0 * $covered / $total;
                if ( $pct < $THRESHOLD ) {
                    push @low_coverage,
                        {
                        class   => $current_class,
                        pct     => $pct,
                        covered => $covered,
                        total   => $total,
                        };
                }
            }
        }

        elsif ( $line =~ m{</class>} ) {
            $current_class = '';
        }
    }

    close($fh);
}

if ( !@low_coverage ) {
    printf "All classes meet the %d%% instruction-coverage threshold.\n", $THRESHOLD;
    exit 0;
}

# Sort by coverage ascending (worst first), then by class name for stability.
@low_coverage =
    sort { $a->{pct} <=> $b->{pct} || $a->{class} cmp $b->{class} } @low_coverage;

printf "Classes with < %d%% instruction coverage (%d found):\n\n",
    $THRESHOLD, scalar @low_coverage;
printf "  %-5s  %-9s  %s\n",   "Cov%", "Covered",    "Class";
printf "  %-5s  %-9s  %s\n",   "-----", "---------",  "-----";
for my $entry (@low_coverage) {
    ( my $display = $entry->{class} ) =~ s{/}{.}g;
    printf "  %4.1f%%  %4d/%-4d  %s\n",
        $entry->{pct},
        $entry->{covered},
        $entry->{total},
        $display;
}
printf "\nTotal: %d classes below %d%% threshold.\n",
    scalar @low_coverage, $THRESHOLD;

exit 0;    # informational only — never fail the build
