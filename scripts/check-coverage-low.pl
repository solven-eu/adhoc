#!/usr/bin/env perl
# Report production classes with < THRESHOLD% instruction coverage from JaCoCo XML reports.
#
# Sources, in order of preference:
#   1. The cross-module aggregate report at `aggregate/target/site/jacoco-aggregate/jacoco.xml`
#      (produced by `mvn jacoco:report-aggregate -pl :adhoc-aggregate -am`). Strongly preferred,
#      because it captures coverage from tests that live in another module than the class under
#      test (e.g. `engine/cube` tests covering `engine/table` classes — those show as 0% in the
#      per-module reports below, even though they are exercised).
#   2. Per-module `*/target/site/jacoco/jacoco.xml` reports (produced by `mvn package` /
#      `mvn verify`). Falls back to these only if the aggregate report is absent.
#
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
my $source = '';

# Prefer the aggregate report when present.
my @aggregate;
find(
    sub {
        push @aggregate, $File::Find::name
            if $_ eq 'jacoco.xml'
            && $File::Find::name =~ m{/target/site/jacoco-aggregate/jacoco\.xml$};
    },
    @roots
);

if (@aggregate) {
    @xml_files = @aggregate;
    $source    = 'aggregate';
}
else {
    find(
        sub {
            push @xml_files, $File::Find::name
                if $_ eq 'jacoco.xml'
                && $File::Find::name =~ m{/target/site/jacoco/jacoco\.xml$};
        },
        @roots
    );
    $source = 'per-module';
}

if ( !@xml_files ) {
    print "No jacoco.xml reports found — run tests first "
        . "(e.g. `mvn jacoco:report-aggregate -pl :adhoc-aggregate -am`).\n";
    exit 0;
}

if ( $source eq 'per-module' ) {
    print "Note: using per-module JaCoCo reports — cross-module test coverage may show as 0%. "
        . "Run `mvn jacoco:report-aggregate -pl :adhoc-aggregate -am` for an accurate picture.\n\n";
}

my @low_coverage;

# JaCoCo writes the report XML on a single line, so the line-by-line + per-line
# regex approach used previously matched only the first <class>/<counter> pair
# per file and reported false negatives. Slurp the whole document instead, then
# walk every <class>...</class> block with a global regex; within each block,
# the LAST <counter type="INSTRUCTION" .../> is the class-level rollup (JaCoCo
# emits per-method counters first, then the class total at the end).
for my $file ( sort @xml_files ) {
    open( my $fh, '<', $file ) or do { warn "Cannot open $file: $!\n"; next };
    my $body = do { local $/; <$fh> };
    close($fh);

    # Ignore self-closing <class .../> tags (empty classes / interfaces with no methods) — without the
    # negative lookbehind on '/', the regex would extend past such a tag and attribute the next class's
    # counters to it.
    while ( $body =~ m{<class\s+name="([^"]+)"[^>]*(?<!/)>(.*?)</class>}gs ) {
        my ( $current_class, $inner ) = ( $1, $2 );

        # Collect every per-counter pair inside the class block; the last one
        # is the class-level rollup.
        my @counters;
        while ( $inner
            =~ m{<counter\s+type="INSTRUCTION"\s+missed="(\d+)"\s+covered="(\d+)"/>}g )
        {
            push @counters, [ $1, $2 ];
        }
        next unless @counters;

        my ( $missed, $covered ) = @{ $counters[-1] };
        my $total = $missed + $covered;
        next unless $total > 0;

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
