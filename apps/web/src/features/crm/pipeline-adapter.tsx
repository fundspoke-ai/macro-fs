import { StaticMarkdownContext } from '@core/component/LexicalMarkdown/component/core/StaticMarkdown';
import { getPermissions } from '@core/component/SharePermissions';
import { ShareTrigger } from '@core/component/TopBar/ShareButton';
import { useShareModal } from '@core/component/TopBar/shareModal';
import { storageServiceClient } from '@service-storage/client';
import { useQueryClient } from '@tanstack/solid-query';
import { Button } from '@ui';
import { okAsync } from 'neverthrow';
import { createSignal, Show, Suspense } from 'solid-js';
import { AddColumnButton } from '../block-database/components/add-column-button';
import { OptionEditingContext } from '../block-database/context/option-editing';
import type { DatabaseViewState } from '../block-database/core/view-state';
import {
  DatabaseMentionPicker,
  DatabaseMentionValue,
  DatabaseTextEditor,
  DatabaseTextValue,
} from '../block-database/database-mentions';
import { DatabaseRecordsView } from '../block-database/views/database-records-view';
import type { Pipeline } from './core/pipeline';
import { createPipelineData } from './queries/pipeline-data';

/** Pipelines supply their own identity and owner to the existing share dialog. */
export function PipelineShare(props: {
  pipeline: Pipeline;
  onCopyLink(): void;
}) {
  const openShare = useShareModal(() => ({
    id: props.pipeline.id,
    blockAlias: 'database',
    itemType: 'crm_pipeline',
    name: props.pipeline.name,
    owner: props.pipeline.userId,
    userPermissions: getPermissions(props.pipeline.grant),
    copyLink: props.onCopyLink,
  }));
  return (
    <ShareTrigger
      id={props.pipeline.id}
      blockType="database"
      onClick={openShare}
      copyLink={props.onCopyLink}
    />
  );
}

/** The CRM adapter supplies authorized data to the reusable records UI. */
export function PipelineDatabaseEditor(props: { pipeline: Pipeline }) {
  return (
    <Suspense
      fallback={<div class="p-4 text-sm text-ink-muted">Loading pipeline…</div>}
    >
      <Show when={props.pipeline.id} keyed>
        {(_id) => <PipelineRecords pipeline={props.pipeline} />}
      </Show>
    </Suspense>
  );
}

function PipelineRecords(props: { pipeline: Pipeline }) {
  const [view, setView] = createSignal<DatabaseViewState>({
    query: { filter: null },
    layout: { kind: 'table', columns: [] },
  });
  const data = createPipelineData(
    { storage: storageServiceClient, client: useQueryClient() },
    () => props.pipeline,
    view
  );
  const canEdit = () =>
    props.pipeline.grant === 'owner' || props.pipeline.grant === 'edit';
  return (
    <StaticMarkdownContext>
      <OptionEditingContext.Provider
        value={canEdit() ? data.optionEditing : undefined}
      >
        <DatabaseRecordsView
          name={props.pipeline.name}
          source={data.source}
          canEdit={canEdit()}
          view={view()}
          stored={false}
          onViewChange={(change) =>
            setView((previous) => ({ ...previous, ...change }))
          }
          boardPositions={{
            state: () => ({ kind: 'ready', positions: [] }),
            setPositions: () => {},
            move: () => okAsync({ positions: [], tableVersion: 0 }),
          }}
          renderTextEditor={(editor) => <DatabaseTextEditor {...editor} />}
          renderTextValue={(value) => <DatabaseTextValue value={value} />}
          renderMentionPicker={(picker) => (
            <DatabaseMentionPicker {...picker} />
          )}
          renderMentionValue={(id, entityType) => (
            <DatabaseMentionValue id={id} entityType={entityType} />
          )}
          createColumn={canEdit() ? data.createColumn : undefined}
          addColumn={
            canEdit()
              ? (label, onCreated) => (
                  <AddColumnButton
                    create={data.createColumn}
                    label={label}
                    onCreated={onCreated}
                  />
                )
              : undefined
          }
          onRenameColumn={canEdit() ? data.renameColumn : undefined}
          onDeleteColumn={canEdit() ? data.deleteColumn : undefined}
          onChangeColumnType={canEdit() ? data.changeColumnType : undefined}
          onReorderColumns={canEdit() ? data.reorderColumns : undefined}
          renderToolbar={(actions) => (
            <div class="flex items-center justify-between gap-3 border-b border-edge-muted px-4 py-2">
              <span class="text-xs text-ink-muted">
                {props.pipeline.recordType === 'company'
                  ? 'Companies'
                  : 'Contacts'}{' '}
                · Customize columns from their headers
              </span>
              <Show when={canEdit()}>
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
      </OptionEditingContext.Provider>
    </StaticMarkdownContext>
  );
}
