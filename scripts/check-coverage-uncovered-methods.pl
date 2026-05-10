#!/usr/bin/env perl
# Report methods with zero instruction coverage in the project-wide aggregate JaCoCo report,
# ordered by LOC (largest first). Useful to surface the chunkiest untested production code
# regardless of which class it lives in — methods whose tests are missing entirely (across
# every module's test suite, not just the home module).
#
# Source: aggregate/target/site/jacoco-aggregate/jacoco.xml — produced by
#         `mvn jacoco:report-aggregate -pl :adhoc-aggregate -am`
#
# Lombok-generated methods (annotated `@lombok.Generated`) are already excluded by JaCoCo's
# built-in filter when `lombok.addLombokGeneratedAnnotation = true` is set in `lombok.config`,
# so they will not appear in the output.
#
# Informational only — always exits 0.
#
# Usage:  perl scripts/check-coverage-uncovered-methods.pl [limit] [class-regex]
#
#   limit         max number of methods to print  (default: 50)
#   class-regex   filter to classes matching this Perl regex (default: .* — no filter)
#
# Examples:
#   perl scripts/check-coverage-uncovered-methods.pl              # top 50 across the whole project
#   perl scripts/check-coverage-uncovered-methods.pl 100          # top 100
#   perl scripts/check-coverage-uncovered-methods.pl 30 'engine/' # top 30 inside engine/* packages

use strict;
use warnings;
use File::Find;

my $REPORT = 'aggregate/target/site/jacoco-aggregate/jacoco.xml';
my $LIMIT  = $ARGV[0] // 50;
my $REGEX  = $ARGV[1] // '.*';

unless ( -f $REPORT ) {
    print STDERR "Aggregate report not found at $REPORT.\n"
        . "Build it first:  mvn jacoco:report-aggregate -pl :adhoc-aggregate -am\n";
    exit 0;
}

# Build a class-name -> module map by walking each module's per-module JaCoCo report. The aggregate report does not
# itself record which module a class came from, so we cross-reference: a class whose name appears in
# `<module>/target/site/jacoco/jacoco.xml` lives in that module. Cheaper and more reliable than re-deriving from
# `<module>/src/main/java` paths (matching package → directory is brittle when modules share package prefixes).
my %classToModule;
find(
    {
        wanted => sub {
            return if $File::Find::name !~ m{/target/site/jacoco/jacoco\.xml$};
            return if $File::Find::name =~ m{/target/site/jacoco-aggregate/};
            ( my $module = $File::Find::name ) =~ s{/target/site/jacoco/jacoco\.xml$}{};
            $module =~ s{^\./}{};
            open( my $mfh, '<', $File::Find::name ) or return;
            my $mbody = do { local $/; <$mfh> };
            close($mfh);
            while ( $mbody =~ m{<class\s+name="([^"]+)"}g ) {
                # First module wins — every class is owned by exactly one module in a sane reactor.
                $classToModule{$1} //= $module;
            }
        },
        no_chdir => 1,
    },
    '.'
);

open( my $fh, '<', $REPORT ) or die "Cannot open $REPORT: $!\n";
my $body = do { local $/; <$fh> };
close($fh);

my @rows;

# Walk every <class>...</class> block, then each <method>...</method> inside.
# JaCoCo emits per-method counters before the class-level rollup, so iterating per
# method block is straightforward.
while ( $body =~ m{<class\s+name="([^"]+)"[^>]*(?<!/)>(.*?)</class>}gs ) {
    my ( $cls, $cls_inner ) = ( $1, $2 );
    next unless $cls =~ /$REGEX/;

    while ( $cls_inner =~ m{<method\s+name="([^"]+)"\s+desc="([^"]+)"[^>]*>(.*?)</method>}gs ) {
        my ( $name, $desc, $m_inner ) = ( $1, $2, $3 );

        # INSTRUCTION counter — primary filter (covered must be 0 to qualify).
        my ( $i_missed, $i_covered ) = ( 0, 0 );
        if ( $m_inner =~ /<counter\s+type="INSTRUCTION"\s+missed="(\d+)"\s+covered="(\d+)"/ ) {
            ( $i_missed, $i_covered ) = ( $1, $2 );
        }
        next unless $i_covered == 0 && $i_missed > 0;

        # LINE counter — used as the LOC sort key. Total lines = missed + covered.
        my ( $l_missed, $l_covered ) = ( 0, 0 );
        if ( $m_inner =~ /<counter\s+type="LINE"\s+missed="(\d+)"\s+covered="(\d+)"/ ) {
            ( $l_missed, $l_covered ) = ( $1, $2 );
        }
        my $loc = $l_missed + $l_covered;

        # Decode XML entities for `<init>` / `<clinit>`.
        ( my $display_name = $name ) =~ s/&lt;/</g;
        $display_name =~ s/&gt;/>/g;

        push @rows,
            {
            class  => $cls,
            module => $classToModule{$cls} // '?',
            method => $display_name,
            desc   => $desc,
            loc    => $loc,
            instr  => $i_missed,
            };
    }
}

# Sort by LOC desc, then by instructions desc (largest method first), then alphabetically for stability.
@rows = sort {
       $b->{loc} <=> $a->{loc}
    || $b->{instr} <=> $a->{instr}
    || $a->{class} cmp $b->{class}
    || $a->{method} cmp $b->{method}
} @rows;

if ( !@rows ) {
    print "No 0%-covered methods found in the aggregate report.\n";
    exit 0;
}

my $shown = $LIMIT < @rows ? $LIMIT : scalar @rows;

printf "0%%-covered methods in aggregate report — showing %d of %d (sorted by LOC desc):\n\n",
    $shown, scalar @rows;

# Compute dynamic widths for module + class so the columns line up regardless of name lengths.
my $mod_width = length('Module');
my $cls_width = length('Class');
for ( my $i = 0; $i < $shown; $i++ ) {
    my $r = $rows[$i];
    ( my $cls_display = $r->{class} ) =~ s{/}{.}g;
    $mod_width = length( $r->{module} ) > $mod_width ? length( $r->{module} ) : $mod_width;
    $cls_width = length($cls_display)   > $cls_width ? length($cls_display)   : $cls_width;
}

printf "  %4s  %5s  %-*s  %-*s  %s\n",
    'LOC', 'Instr', $mod_width, 'Module', $cls_width, 'Class', 'Method';
printf "  %4s  %5s  %-*s  %-*s  %s\n",
    '----', '-----', $mod_width, '-' x $mod_width, $cls_width, '-' x $cls_width, '------';

for ( my $i = 0; $i < $shown; $i++ ) {
    my $r = $rows[$i];
    ( my $cls_display = $r->{class} ) =~ s{/}{.}g;
    printf "  %4d  %5d  %-*s  %-*s  %s%s\n",
        $r->{loc}, $r->{instr},
        $mod_width, $r->{module},
        $cls_width, $cls_display,
        $r->{method}, $r->{desc};
}

if ( $shown < @rows ) {
    printf "\n… %d more methods not shown. Pass a higher limit as 1st arg.\n",
        scalar @rows - $shown;
}

exit 0;
