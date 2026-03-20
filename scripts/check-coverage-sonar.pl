#!/usr/bin/env perl
# Report files with < THRESHOLD% line coverage by querying the SonarCloud REST API.
# No local build required — works offline from any machine with internet access and curl.
#
# Usage:
#   perl scripts/check-coverage-sonar.pl [options]
#
# Options:
#   --component KEY   SonarCloud component/project key  (default: adhoc)
#   --threshold N     Flag files below N% line coverage  (default: 20)
#   --token TOKEN     API token for private projects     (default: none)
#
# Requires: curl (any modern version), perl 5.14+ (JSON::PP is in Perl core).

use strict;
use warnings;
use JSON::PP qw(decode_json);
use Getopt::Long qw(GetOptions);

# ── Configuration ──────────────────────────────────────────────────────────────

my $component = 'adhoc';
my $threshold = 20;
my $token     = '';

GetOptions(
    'component=s' => \$component,
    'threshold=i' => \$threshold,
    'token=s'     => \$token,
) or die "Usage: $0 [--component KEY] [--threshold N] [--token TOKEN]\n";

# ── Fetch all file measures from SonarCloud (handles pagination) ────────────────

my $base = 'https://sonarcloud.io/api/measures/component_tree'
    . "?component=$component"
    . '&metricKeys=line_coverage'
    . '&qualifiers=FIL'
    . '&strategy=leaves'
    . '&ps=500';

my @low;
my ( $page, $total_pages ) = ( 1, 1 );

while ( $page <= $total_pages ) {
    my $url  = "$base&p=$page";
    my @curl = ( 'curl', '-sf', '--max-time', '30' );
    push @curl, ( '-u', "$token:" ) if $token;
    push @curl, $url;

    # open(-|, LIST) is the safe way to capture output without shell expansion
    open( my $fh, '-|', @curl )
        or die "Cannot launch curl: $!\n";
    my $body = do { local $/; <$fh> };
    close($fh);

    if ( $? != 0 ) {
        die "curl exited with status $? for URL: $url\n";
    }
    if ( !$body ) {
        die "Empty response from SonarCloud — check the component key ('$component') and network access.\n";
    }

    my $data = decode_json($body);

    # On first page compute total number of pages
    if ( $page == 1 ) {
        my $paging = $data->{paging} // {};
        my $total  = $paging->{total}    // 0;
        my $ps     = $paging->{pageSize} // 500;
        $total_pages = $ps > 0 ? int( ( $total + $ps - 1 ) / $ps ) : 1;
        $total_pages = 1 if $total_pages < 1;
        printf "Fetching %d file(s) from SonarCloud (component: %s) …\n",
            $total, $component;
    }

    for my $comp ( @{ $data->{components} // [] } ) {
        my $path = $comp->{path} // $comp->{name} // '?';
        for my $m ( @{ $comp->{measures} // [] } ) {
            next unless $m->{metric} eq 'line_coverage';
            my $pct = $m->{value} + 0;    # SonarCloud returns a string
            push @low, { path => $path, pct => $pct }
                if $pct < $threshold;
        }
    }

    $page++;
}

# ── Report ─────────────────────────────────────────────────────────────────────

if ( !@low ) {
    printf "All analysed files meet the %d%% line-coverage threshold.\n",
        $threshold;
    exit 0;
}

@low = sort { $a->{pct} <=> $b->{pct} || $a->{path} cmp $b->{path} } @low;

printf "\nFiles with < %d%% line coverage (%d found):\n\n",
    $threshold, scalar @low;
printf "  %-5s  %s\n", "Cov%", "File";
printf "  %-5s  %s\n", "-----", "----";
for my $e (@low) {
    printf "  %4.1f%%  %s\n", $e->{pct}, $e->{path};
}
printf "\nTotal: %d file(s) below %d%% threshold.\n",
    scalar @low, $threshold;

exit 0;    # informational only — never fail the build
