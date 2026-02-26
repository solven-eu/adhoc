# Secrets management

## ~`ANTHROPIC_API_KEY`~ `CLAUDE_CODE_OAUTH_TOKEN` Used for Claude Code AI Agents

One can generate a 1-year token, usable as `CLAUDE_CODE_OAUTH_TOKEN` with a local CLI: 
```
claude setup-token
```

It is a workaround as:

```
  The OAuth token from your Max subscription (claude setup-token) can be set as a GitHub Actions secret, but it only lives ~24 hours — so
  scheduled nightly workflows will fail once it expires. This is a https://github.com/anthropics/claude-code-action/issues/727 with no fix yet.

  Your options right now:
  1. Top up API credits at https://console.anthropic.com — cheapest for occasional nightly runs (a few dollars/month).
  2. Rotate the OAuth token manually before each run — impractical.
  3. Wait for Anthropic to ship refresh-token support for Max subscribers in GitHub Actions
  (https://github.com/anthropics/claude-code-action/issues/727).

  Option 1 is the only practical one today.

  Sources:
  - https://github.com/anthropics/claude-code-action/issues/4
  - https://github.com/anthropics/claude-code-action/issues/727
  - https://support.claude.com/en/articles/11145838-using-claude-code-with-your-pro-or-max-plan
```