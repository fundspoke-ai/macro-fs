import { getPermissions } from '@core/component/SharePermissions';
import { ShareTrigger } from '@core/component/TopBar/ShareButton';
import { useShareModal } from '@core/component/TopBar/shareModal';
import { queryReadyGate } from '@queries/gate';
import { useDatabaseDetailQuery } from '@queries/storage/databases';
import { useDatabaseTableChangedSync } from '@queries/storage/databases-sync';
import type { DatabaseView } from '@service-storage/generated/schemas/databaseView';
import { Button } from '@ui';
import { createSignal, Show, Suspense } from 'solid-js';
import { DatabaseGrid } from '../block-database/component/DatabaseGrid';
import { allRecordsView } from '../block-database/core/views';
import type { Pipeline } from './core/pipeline';

/** Pipelines use the same sharing surface as databases and other entities. */
export function PipelineShare(props: {
  pipeline: Pipeline;
  onCopyLink(): void;
}) {
  const database = useDatabaseDetailQuery(() => props.pipeline.databaseId);
  const openShare = useShareModal(() => {
    if (!queryReadyGate(database)) return;
    return {
      id: props.pipeline.id,
      blockAlias: 'database',
      itemType: 'crm_pipeline',
      name: props.pipeline.name,
      owner: database.data.database.owner_id,
      userPermissions: getPermissions(props.pipeline.grant),
      copyLink: props.onCopyLink,
    };
  });
  return (
    <ShareTrigger
      id={props.pipeline.id}
      blockType="database"
      onClick={openShare}
      copyLink={props.onCopyLink}
    />
  );
}

/** Compose the existing database editor without depending on a legacy block. */
export function PipelineDatabaseEditor(props: { pipeline: Pipeline }) {
  const query = useDatabaseDetailQuery(() => props.pipeline.databaseId);
  useDatabaseTableChangedSync(() => props.pipeline.databaseId);
  const table = () =>
    queryReadyGate(query)
      ? query.data.tables.find(
          (table) => table.table.id === props.pipeline.tableId
        )
      : undefined;
  const [view, setView] = createSignal<DatabaseView>(
    allRecordsView({
      id: props.pipeline.tableId,
      database_id: props.pipeline.databaseId,
    })
  );
  return (
    <div class="flex size-full min-h-0 flex-col">
      <Show when={query.isLoading}>
        <div class="p-4 text-sm text-ink-muted">Loading pipeline…</div>
      </Show>
      <Show when={query.isError}>
        <div role="alert" class="p-4 text-sm text-ink-muted">
          Could not load this pipeline.
          <Button variant="ghost" onClick={() => void query.refetch()}>
            Retry
          </Button>
        </div>
      </Show>
      <Suspense
        fallback={
          <div class="p-4 text-sm text-ink-muted">Loading pipeline…</div>
        }
      >
        <Show when={table()}>
          {(current) => (
            <DatabaseGrid
              databaseId={props.pipeline.databaseId}
              table={current()}
              primaryColumnId={props.pipeline.primaryColumnId}
              canEdit={
                props.pipeline.grant === 'owner' ||
                props.pipeline.grant === 'edit'
              }
              view={view()}
              stored={false}
              onViewChange={(change) =>
                setView((previous) => ({ ...previous, ...change }))
              }
              renderToolbar={(actions) => (
                <div class="flex items-center justify-between gap-3 border-b border-edge-muted px-4 py-2">
                  <span class="text-xs text-ink-muted">
                    {props.pipeline.recordType === 'company'
                      ? 'Companies'
                      : 'Contacts'}{' '}
                    · Customize columns from their headers
                  </span>
                  <Show
                    when={
                      props.pipeline.grant === 'owner' ||
                      props.pipeline.grant === 'edit'
                    }
                  >
                    <Button
                      size="sm"
                      disabled={actions.pending()}
                      onClick={() => void actions.createRecord()}
                    >
                      {props.pipeline.recordType === 'company'
                        ? 'Add company'
                        : 'Add contact'}
                    </Button>
                  </Show>
                </div>
              )}
            />
          )}
        </Show>
      </Suspense>
    </div>
  );
}
