import type { TableDetail } from '@service-storage/generated/schemas/tableDetail';
import { cleanup, render, waitFor } from '@solidjs/testing-library';
import { QueryClient, QueryClientProvider } from '@tanstack/solid-query';
import { okAsync } from 'neverthrow';
import { Suspense } from 'solid-js';
import { afterEach, expect, it, vi } from 'vitest';
import type { Pipeline } from '../core/pipeline';
import { createPipelineData, pipelineCell } from './pipeline-data';

const pipeline: Pipeline = {
  id: 'pipeline',
  databaseId: 'storage-only',
  tableId: 'records',
  primaryColumnId: 'company',
  name: 'Sales',
  userId: 'macro|owner@example.com',
  teamId: 'team',
  recordType: 'company',
  sharing: 'private',
  grant: 'owner',
  createdAt: '2026-10-01T00:00:00Z',
  trashedAt: null,
};
const table: TableDetail = {
  sql_name: 'Records',
  table: {
    id: 'records',
    database_id: 'storage-only',
    name: 'Records',
    position: 'a0',
    version: 1,
  },
  views: [],
  columns: [
    {
      sql_name: 'Company',
      writable: true,
      shared_outside_database: false,
      column: {
        id: 'company',
        table_id: 'records',
        property_definition_id: 'definition',
        position: 'a0',
        config: null,
        display_name: null,
        infer_type: false,
      },
      definition: {
        definition: {
          id: 'definition',
          display_name: 'Company',
          data_type: 'ENTITY',
          specific_entity_type: 'COMPANY',
          is_multi_select: false,
          is_system: false,
          is_metadata: false,
          owner: { scope: 'database', database_id: 'storage-only' },
          created_at: '2026-10-01T00:00:00Z',
          updated_at: '2026-10-01T00:00:00Z',
        },
        property_options: [],
      },
    },
  ],
};
const clients: QueryClient[] = [];
afterEach(() => {
  cleanup();
  for (const client of clients.splice(0)) client.clear();
});

function setup(writable = true) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  const storage = {
    getCrmPipelineTable: vi.fn(async () =>
      okAsync({
        ...table,
        columns: table.columns.map((column) => ({ ...column, writable })),
      })
    ),
    getCrmPipelineRows: vi.fn(async (_id: string, after?: string) =>
      okAsync({
        rows: [
          {
            rowId: after ? 'row2' : 'row1',
            cells: {
              company: {
                type: 'EntityReference' as const,
                value: [
                  {
                    entity_id: after ? 'company2' : 'company1',
                    entity_type: 'COMPANY' as const,
                    specific_message_id: null,
                  },
                ],
              },
            },
          },
        ],
        next: after ? null : 'row1',
        version: 1,
      })
    ),
    applyCrmPipelineOps: vi.fn(() =>
      okAsync({
        results: [
          {
            kind: 'rows' as const,
            table: 'records',
            tableVersion: 2,
            change: { kind: 'inserted' as const, rows: ['row3'] },
          },
        ],
        changes: [],
      })
    ),
  };
  let data!: ReturnType<typeof createPipelineData>;
  function Host() {
    data = createPipelineData(
      { client, storage },
      () => pipeline,
      () => ({
        query: { filter: null },
        layout: { kind: 'table', columns: [] },
      })
    );
    return <div>{data.source.snapshot()?.rows.length ?? 'loading'}</div>;
  }
  render(() => (
    <QueryClientProvider client={client}>
      <Suspense fallback="loading">
        <Host />
      </Suspense>
    </QueryClientProvider>
  ));
  return { storage, data: () => data, client };
}

it('reads all pages through pipeline identity and supplies primary references to the shared grid', async () => {
  const host = setup();
  await waitFor(() =>
    expect(host.data().source.snapshot()?.rows).toHaveLength(2)
  );
  expect(host.storage.getCrmPipelineTable).toHaveBeenCalledWith('pipeline');
  expect(host.storage.getCrmPipelineRows).toHaveBeenCalledWith(
    'pipeline',
    'row1'
  );
  expect(host.data().source.columns()[0].primary).toBe(true);
  expect(host.data().source.snapshot()?.rows[1].cells.company).toBe('company2');
  expect(host.data().source.read()).toBeUndefined();
});

it('writes references through the pipeline operation endpoint and returns the inserted row identity', async () => {
  const host = setup();
  await waitFor(() => expect(host.data().source.snapshot()).toBeDefined());
  const result = await host
    .data()
    .source.write(
      { kind: 'create', values: { company: 'company3' } },
      undefined,
      false
    );
  expect(result.isOk() && result.value.insertedRowIds).toEqual(['row3']);
  expect(host.storage.applyCrmPipelineOps).toHaveBeenCalledWith('pipeline', {
    ops: [
      {
        kind: 'rows',
        table: 'records',
        change: {
          kind: 'insert',
          rows: [
            [
              {
                column: 'company',
                value: {
                  type: 'entities',
                  value: [{ entityType: 'COMPANY', entityId: 'company3' }],
                },
              },
            ],
          ],
        },
      },
    ],
  });
});

it('refuses a write to a read-only source before calling storage', async () => {
  const host = setup(false);
  await waitFor(() => expect(host.data().source.snapshot()).toBeDefined());
  const result = await host
    .data()
    .source.write(
      { kind: 'cell', rowId: 'row1', columnId: 'company', value: 'other' },
      undefined,
      false
    );
  expect(result.isErr() && result.error.kind).toBe('read-only-column');
  expect(host.storage.applyCrmPipelineOps).not.toHaveBeenCalled();
});

it('maps single and multiple entity values without leaking transport objects into cells', () => {
  const column = table.columns[0];
  const value = {
    type: 'EntityReference' as const,
    value: [
      {
        entity_id: 'one',
        entity_type: 'COMPANY' as const,
        specific_message_id: null,
      },
    ],
  };
  expect(pipelineCell(value, column)).toBe('one');
  expect(
    pipelineCell(value, {
      ...column,
      definition: {
        ...column.definition,
        definition: { ...column.definition.definition, is_multi_select: true },
      },
    })
  ).toBe('["one"]');
  expect(pipelineCell(undefined, column)).toBeNull();
});
