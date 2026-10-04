# Legacy CRM → Sales

This PR includes the operator command `crm_sales_backfill`. The schema migration
creates its completion table; **deploying the schema does not run the data copy**.
The command uses `DATABASE_URL` through the standard configuration loader.

For each CRM-enabled team with visible companies, it creates one team-shared,
renameable **Sales** company pipeline. The team owner owns it; members can edit it.
It copies every visible company, including Customer, Churned and unstaged records,
with its current Stage, Owner and Revenue. Custom stage order and labels are
copied into independent pipeline options; old stage IDs resolve through the team's
legacy mapping, then the same label fallback as the CRM. Unmappable stages or
invalid values fail that team's import instead of silently dropping data.

Hidden companies and disabled CRMs are skipped. Contacts, activity, company
details and custom properties remain on their original CRM entities, linked from
the pipeline's Company reference. User-private properties are never copied into
the team-shared database. This change does not convert company Stage to Status or
retarget the old CRM/AI writers; those require a separate cutover.

## Run

Apply all schema migrations and deploy the pipeline-capable application first.
Use a rehearsal database to validate representative teams. For the final copy,
pause legacy CRM writes (including AI tools, imports and jobs) and keep them
paused through the application's writer cutover. **This is a one-time copy, not
ongoing synchronization**: later company edits do not update the imported row.
Do not run the final production copy while old writers remain the active workflow.

From the repository root inside Nix, with the target `DATABASE_URL` configured:

```bash
cargo run -p crm --no-default-features --features sales-backfill \
  --bin crm_sales_backfill -- --check

cargo run -p crm --no-default-features --features sales-backfill \
  --bin crm_sales_backfill --
```

Add `--team <team-uuid>` to either command for one team. `--check` executes the
same copy and constraint checks, then rolls back; it needs write privileges and
takes the same locks as a run. Both print one JSON report per team. Errors stop
the command with a failing exit code; previously committed teams remain complete.
Repair the reported source issue and rerun. Empty teams get no completion marker
and can be retried after they acquire visible companies.

Companies are read in pages of 250; each team's pipeline, rows, grants and marker
commit together. Concurrent attempts serialize through the existing team-sharing
guard. That guard also delays other team-sharing/membership operations while a
team is being copied, so schedule the final run in the cutover window.

The marker is keyed by team identity, not the name Sales. An existing user-created
Sales pipeline is untouched; the migrated pipeline is separate. Reruns skip completed
teams and preserve subsequent edits, row deletions, rename, sharing and trash state.
Hard-deleting the pipeline leaves its completion marker, preventing resurrection.
Do not delete these markers to attempt a retry after a successful import.

## Verify and roll back

Compare each report's company count to the visible companies for its team. Open
Sales as the owner and a member; check company links and Stage/Owner/Revenue on
representative open, closed and unstaged rows. Verify a nonmember cannot access it.

The legacy records and values are never changed. A failed team's transaction
leaves no pipeline or checkpoint. If a committed import must be withdrawn before
cutover, trash its pipeline through the normal owner controls and resume the
legacy workflow. Preserve the marker. The checkpoint migration's down script
only removes tracking, so use it only when reverting the feature and retiring the
operator command; dropping tracking while continuing to run the command permits
duplicate imports.
