# STATE.md — current position

- **R1/R2:** complete at `af688436216a84225eb494fb08f7c7a3057f1349`
  (PRs [#1](https://github.com/fakoli/anvil-events/pull/1) and
  [#2](https://github.com/fakoli/anvil-events/pull/2)). The one formal review
  was closed and its code blockers were fixed.
- **R3:** complete in public Anvil Serving. The lifecycle seam merged in PRs
  #417/#418; the reviewed fixes are at
  `105167ec61b309e55353b00e10e5bb2d8bd8e4cb`, with exact-head CI green.
- **R4:** merged in PR #5 at
  `08468f673102419c3c3f17f69267d634bccc399d`. Public source adds
  secret-preserving JSON merge, allowlisted argv-only command configuration,
  and an authenticated immutable-artifact publisher. The review blocker was
  fixed in PR #6 and the private canary/rollback sequence passed.
- **Formal R4 review:** the one merged-result `gpt-5.6-sol` review
  (`01a00c9f-ee53-7711-9c54-ee45266ccc0c`) returned `REQUEST_CHANGES` for one
  code blocker: a slow header sender could renew the serial loopback HTTP
  server's timeout indefinitely. The fix adds an absolute header deadline and
  a loopback starvation regression; no second formal review is permitted.
- **R5:** startup drift verification/repair merged in PR #7. The one formal
  R5 review found an indeterminate-retry blocker; PR #8 fixed it without a
  second formal review. PR #9 released v0.2.1.
- **Live acceptance correction:** an equivalent desired event exposed an
  outcome-ledger idempotency collision without reapplying the resource. PR #10
  added per-event outcome keys and the regression test, then released v0.2.2
  at `c1a08bd82d7f43da7f166a43aab2818c84a79360`.
- **Current public gates:** 161 hermetic tests, `ruff`, package validation,
  Compose acceptance, and the Windows/macOS/Linux Python 3.11/3.13 matrix pass
  on the exact v0.2.2 source.
- **Deployment:** v0.2.2 is installed on the four-node private reference
  topology. mTLS identity/ACL checks, authenticated immutable artifacts,
  offline durable catch-up, startup drift repair, duplicate delivery,
  broker-loss recovery, and real primary-client turns passed. Private
  identities, routes, rollback paths, and raw evidence are not copied here.

## Next actions

1. Replace the reference authority node's user-logon autostart fallback with a
   boot-time service under an elevated operator change.
2. Connect the public lifecycle seam to automatic desired-generation
   publication after a separately approved router-change transaction.
3. Exercise a new immutable generation through the same staged canary and
   rollback contract before treating automatic publication as routine.
