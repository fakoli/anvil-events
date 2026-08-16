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
  and an authenticated immutable-artifact publisher. Private topology and
  manifests are being prepared in an isolated private worktree.
- **Formal R4 review:** the one merged-result `gpt-5.6-sol` review
  (`01a00c9f-ee53-7711-9c54-ee45266ccc0c`) returned `REQUEST_CHANGES` for one
  code blocker: a slow header sender could renew the serial loopback HTTP
  server's timeout indefinitely. The fix adds an absolute header deadline and
  a loopback starvation regression; no second formal review is permitted.
- **Current public gates:** 154 hermetic tests, `ruff`, wheel/sdist build, and
  clean-wheel CLI smoke pass. The review-fix exact-head CI is pending.
- **Deployment:** authorized by the operator but not started. No package,
  broker, certificate, service, client configuration, route, or ingress change
  from R4/R5 has been applied to a live host.

## Next actions

1. Merge the R4 review fix and require exact-head CI.
2. Release the exact reviewed wheel, canary it on the non-authoritative node,
   prove disconnect/catch-up and rollback, then stage the remaining fleet.
