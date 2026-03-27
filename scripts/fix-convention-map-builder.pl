#!/usr/bin/env perl
# Rewrite Map.of() / ImmutableMap.of() calls that are multi-line AND have 2+
# entries to ImmutableMap.builder().put(k, v)....build(), and patch imports.
# Single-line calls are left untouched regardless of entry count.
#
# Uses a full tokeniser (string literals, char literals, line/block comments)
# so commas inside strings or commented-out arguments never cause false positives.
#
# Nested violations are fixed on subsequent runs (outer call is fixed first,
# leaving any inner violation for the next pass).
#
# Usage:  perl scripts/fix-convention-map-builder.pl [--dry-run] [file|dir ...]
#         Directories are searched recursively for *.java.
#         --dry-run / -n  Report what would change without writing files.
# Exit:   0 = nothing to fix, 1 = files were (or would be) modified.

use strict;
use warnings;
use File::Find;

my $dry_run = grep { $_ eq '--dry-run' || $_ eq '-n' } @ARGV;
my @targets  = grep { $_ ne '--dry-run' && $_ ne '-n'  } @ARGV;
@targets = ('.') unless @targets;

my @files;
for my $t (@targets) {
    if   (-f $t) { push @files, $t }
    else { find(sub { push @files, $File::Find::name if /\.java$/ }, $t) }
}

my $total = 0;
for my $file (sort @files) {
    open my $fh, '<', $file or die "Cannot open $file: $!";
    local $/;
    my $orig = <$fh>;
    close $fh;

    my ($result, $n) = fix_content($orig);
    next unless $n;

    $total++;
    printf "%s: %d rewrite(s)%s\n", $file, $n, $dry_run ? ' [dry-run]' : '';
    unless ($dry_run) {
        open my $out, '>', $file or die "Cannot write $file: $!";
        print $out $result;
        close $out;
    }
}

printf "\n%d file(s) %smodified.\n", $total, $dry_run ? 'would be ' : '' if $total;
exit($total ? 1 : 0);

# ============================================================
# Core logic
# ============================================================

sub fix_content {
    my ($content) = @_;
    my @repls;    # each entry: [$start, $end, $replacement_text]

    my $pos = 0;
    my $len = length $content;

    while ($pos < $len) {
        my $ch = substr($content, $pos, 1);

        # --- Skip constructs that mask commas / parens ---

        if ($ch eq '"') {
            $pos = substr($content, $pos, 3) eq '"""'
                 ? skip_text_block($content, $pos)
                 : skip_string($content, $pos);
            next;
        }
        if ($ch eq "'") { $pos = skip_char_lit($content, $pos); next }

        if ($pos + 1 < $len) {
            my $two = substr($content, $pos, 2);
            if ($two eq '//') { $pos = skip_line_comment($content,  $pos); next }
            if ($two eq '/*') { $pos = skip_block_comment($content, $pos); next }
        }

        # --- Detect (Immutable)?Map.of( ---
        # Guard: char before must not be a word character (avoid matching
        # something like FooMap.of).
        if ($pos == 0 || substr($content, $pos - 1, 1) !~ /[A-Za-z0-9_]/) {
            if (substr($content, $pos) =~ /\A((?:Immutable)?Map)\s*\.\s*of\s*\(/) {
                my $call_start = $pos;
                my $args_start = $pos + length($&);    # position just after '('

                my ($args_end, @args) = extract_args($content, $args_start);

                # Need an even number of args >= 4 (i.e. 2+ key/value pairs).
                if (@args >= 4 && @args % 2 == 0) {
                    my $span = substr($content, $call_start, $args_end - $call_start);
                    if ($span =~ /\n/) {
                        push @repls, [$call_start, $args_end, build_repl(\@args)];
                    }
                }

                # Advance past the whole call (nested violations handled next run).
                $pos = $args_end;
                next;
            }
        }

        $pos++;
    }

    return ($content, 0) unless @repls;

    # Apply replacements right-to-left so earlier offsets stay valid.
    my $result = $content;
    for my $r (reverse @repls) {
        substr($result, $r->[0], $r->[1] - $r->[0]) = $r->[2];
    }

    $result = fix_imports($result);
    return ($result, scalar @repls);
}

# Build the ImmutableMap.builder()...build() replacement string.
# Spotless is responsible for final formatting; we produce a single-line form
# that is always syntactically valid.
sub build_repl {
    my ($args) = @_;
    my $s = 'ImmutableMap.builder()';
    for (my $i = 0; $i < @$args; $i += 2) {
        $s .= '.put(' . trim($args->[$i]) . ', ' . trim($args->[$i + 1]) . ')';
    }
    $s .= '.build()';
    return $s;
}

# Ensure ImmutableMap is imported.  We add it in the most natural location and
# rely on Spotless to sort the import block afterwards.
sub fix_imports {
    my ($content) = @_;

    # Already imported — nothing to do.
    return $content
        if $content =~ /\bimport\s+com\.google\.common\.collect\.ImmutableMap\s*;/;

    my $new_import = "import com.google.common.collect.ImmutableMap;\n";

    # Helper: insert $new_import after the last line matching $pat in $content.
    for my $pat (
        qr/^import\s+com\.google\.common\.collect\.[^;]+;\n/m,
        qr/^import\s+com\.google\.[^;]+;\n/m,
        qr/^import\s+java\.[^;]+;\n/m,
    ) {
        if ($content =~ $pat) {
            my $last_end = 0;
            $last_end = pos($content) while $content =~ /$pat/g;
            substr($content, $last_end, 0) = $new_import if $last_end;
            return $content;
        }
    }

    # Last resort: insert after the package declaration.
    $content =~ s/(^package\s+[^;]+;\n)/$1\n$new_import/m;
    return $content;
}

# ============================================================
# Tokeniser helpers
# ============================================================

# Advance past a regular string literal (opening " already at $p).
sub skip_string {
    my ($s, $p) = @_;
    $p++;    # skip opening "
    my $len = length $s;
    while ($p < $len) {
        my $c = substr($s, $p, 1);
        if ($c eq '\\') { $p += 2; next }    # escaped character
        if ($c eq '"')  { return $p + 1 }    # closing "
        $p++;
    }
    return $p;
}

# Advance past a text block (opening """ already at $p).
sub skip_text_block {
    my ($s, $p) = @_;
    $p += 3;    # skip opening """
    my $len = length $s;
    while ($p <= $len - 3) {
        return $p + 3 if substr($s, $p, 3) eq '"""';
        $p++;
    }
    return $len;
}

# Advance past a character literal (opening ' already at $p).
sub skip_char_lit {
    my ($s, $p) = @_;
    $p++;
    my $len = length $s;
    while ($p < $len) {
        my $c = substr($s, $p, 1);
        if ($c eq '\\') { $p += 2; next }
        if ($c eq "'")  { return $p + 1 }
        $p++;
    }
    return $p;
}

# Advance past a // comment (including the trailing newline).
sub skip_line_comment {
    my ($s, $p) = @_;
    my $nl = index($s, "\n", $p);
    return $nl < 0 ? length($s) : $nl + 1;
}

# Advance past a /* ... */ comment.
sub skip_block_comment {
    my ($s, $p) = @_;
    my $end = index($s, '*/', $p + 2);
    return $end < 0 ? length($s) : $end + 2;
}

# Extract the top-level comma-separated arguments of a call, starting at $p
# (the position immediately after the opening '(').
# Returns ($end_pos, @args) where $end_pos is immediately after the closing ')'.
# Uses the full tokeniser so commas inside strings/comments/nested parens are
# correctly ignored.
sub extract_args {
    my ($s, $p) = @_;
    my $len   = length $s;
    my $depth = 1;
    my @args;
    my $arg_start = $p;

    while ($p < $len) {
        my $ch = substr($s, $p, 1);

        # Skip masking constructs (same logic as the outer scanner).
        if ($ch eq '"') {
            $p = substr($s, $p, 3) eq '"""'
               ? skip_text_block($s, $p)
               : skip_string($s, $p);
            next;
        }
        if ($ch eq "'") { $p = skip_char_lit($s, $p); next }
        if ($p + 1 < $len) {
            my $two = substr($s, $p, 2);
            if ($two eq '//') { $p = skip_line_comment($s,  $p); next }
            if ($two eq '/*') { $p = skip_block_comment($s, $p); next }
        }

        # Depth tracking for (, [, {  and ), ], }
        if ($ch =~ /[(\[{]/) { $depth++; $p++; next }
        if ($ch =~ /[)\]}]/) {
            $depth--;
            if ($depth == 0) {
                # Closing paren of the Map.of(  call itself.
                push @args, substr($s, $arg_start, $p - $arg_start);
                return ($p + 1, @args);
            }
            $p++; next;
        }

        # Top-level comma: end of current argument.
        if ($ch eq ',' && $depth == 1) {
            push @args, substr($s, $arg_start, $p - $arg_start);
            $p++;
            $arg_start = $p;
            next;
        }

        $p++;
    }

    # Unterminated call (parse error in source) — return whatever we have.
    return ($p, @args);
}

# Strip leading and trailing whitespace (including newlines).
sub trim {
    my $s = $_[0];
    $s =~ s/\A\s+//;
    $s =~ s/\s+\z//;
    return $s;
}
