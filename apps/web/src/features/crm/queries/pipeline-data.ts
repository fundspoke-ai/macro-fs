import type { DatabaseOp } from '@core/database-sql/generated/types';
import { throwOnErr } from '@core/util/result';
import { queryReadyGate } from '@queries/gate';
import type { ColumnDetail } from '@service-storage/generated/schemas/columnDetail';
import type { PropertyValue } from '@service-storage/generated/schemas/propertyValue';
import { useQuery } from '@tanstack/solid-query';
import { err, errAsync, ok, okAsync, ResultAsync } from 'neverthrow';
import type { Accessor } from 'solid-js';
import { match } from 'ts-pattern';
import { v7 as uuidv7 } from 'uuid';
import type { OptionEditing } from '../../block-database/context/option-editing';
import type { DatabaseRowsSource } from '../../block-database/context/table-source';
import {
  missingOptionLabels,
  mutationOp,
} from '../../block-database/core/cell-ops';
import type { DatabaseColumnTypeChange } from '../../block-database/core/column-schema';
import type { DatabaseCellValue } from '../../block-database/core/database-view';
import { UNAVAILABLE_OPTION } from '../../block-database/core/grid-cells';
import { defaultDatabaseColumnName } from '../../block-database/core/property-creation';
import type { DatabaseRow } from '../../block-database/core/table';
import type { DatabaseViewState } from '../../block-database/core/view-state';
import type { DatabaseWriteFailure } from '../../block-database/core/write-failure';
import { toViewColumn } from '../../block-database/queries/column-detail';
import { opColumnKind } from '../../block-database/queries/column-schema';
import type { Pipeline } from '../core/pipeline';
import type { CrmRecordDependencies } from './dependencies';
import { crmKeys } from './keys';

/** Adapt stored properties to the shared editor's values. */
export function pipelineCell(
  value: PropertyValue | undefined,
  column: ColumnDetail
): DatabaseCellValue {
  if (!value) return null;
  const listed = (values: string[]) =>
    column.definition.definition.is_multi_select
      ? JSON.stringify(values)
      : (values[0] ?? null);
  return match(value)
    .with({ type: 'Boolean' }, ({ value }) => (value ? 1 : 0))
    .with(
      { type: 'Number' },
      { type: 'String' },
      { type: 'Date' },
      ({ value }) => value
    )
    .with({ type: 'Link' }, ({ value }) => listed(value))
    .with({ type: 'EntityReference' }, ({ value }) =>
      listed(value.map((ref) => ref.entity_id))
    )
    .with({ type: 'SelectOption' }, ({ value }) =>
      listed(
        value.map((id) => {
          const option = column.definition.property_options.find(
            (option) => option.id === id
          );
          return option ? String(option.value.value) : UNAVAILABLE_OPTION;
        })
      )
    )
    .exhaustive();
}

/** CRM owns every read and write; the records view receives only capabilities. */
export function createPipelineData(
  deps: {
    client: CrmRecordDependencies['client'];
    storage: Pick<
      CrmRecordDependencies['storage'],
      'getCrmPipelineTable' | 'getCrmPipelineRows' | 'applyCrmPipelineOps'
    >;
  },
  pipeline: Accessor<Pipeline>,
  view: Accessor<DatabaseViewState>
) {
  const id = pipeline().id;
  const tableId = pipeline().tableId;
  const table = useQuery(
    () => ({
      queryKey: crmKeys.pipelineTable(id).queryKey,
      queryFn: () => throwOnErr(() => deps.storage.getCrmPipelineTable(id)),
      refetchInterval: 5000,
    }),
    () => deps.client
  );
  const columns = () => (queryReadyGate(table) ? table.data.columns : []);
  const rows = useQuery(
    () => ({
      queryKey: crmKeys.pipelineRows(id).queryKey,
      queryFn: async () => {
        // Restart a paginated read if another writer changed the table between pages.
        for (let attempt = 0; attempt < 3; attempt++) {
          let after: string | undefined;
          let version: number | undefined;
          const records: {
            rowId: string;
            cells: Record<string, PropertyValue>;
          }[] = [];
          do {
            const page = await throwOnErr(() =>
              deps.storage.getCrmPipelineRows(id, after)
            );
            if (version !== undefined && version !== page.version) break;
            version = page.version;
            records.push(...page.rows);
            after = page.next ?? undefined;
            if (!after) return { records, version };
          } while (after);
        }
        throw new Error('Pipeline changed while loading. Please retry.');
      },
      refetchInterval: 5000,
    }),
    () => deps.client
  );
  const refresh = async () => {
    await Promise.all([
      deps.client.invalidateQueries({
        queryKey: crmKeys.pipelineTable(id).queryKey,
      }),
      deps.client.invalidateQueries({
        queryKey: crmKeys.pipelineRows(id).queryKey,
      }),
    ]);
  };
  const apply = (ops: DatabaseOp[], baseVersion?: number) =>
    deps.storage
      .applyCrmPipelineOps(id, {
        ops,
        ...(baseVersion !== undefined
          ? { baseVersions: { [tableId]: baseVersion } }
          : {}),
      })
      .mapErr(([error]) => error)
      .andThen((applied) =>
        // Refetch errors belong to the queries; a committed write must never be retried as a failed write.
        ResultAsync.fromPromise(refresh(), () => undefined)
          .orElse(() => okAsync(undefined))
          .map(() => applied)
      );
  const addOption = (column: string, label: string) =>
    apply([
      {
        kind: 'column',
        table: tableId,
        column,
        change: { kind: 'add_options', options: [{ id: uuidv7(), label }] },
      },
    ]).map(() => undefined);
  const source: DatabaseRowsSource = {
    columns: () =>
      columns().map((column) => ({
        ...toViewColumn(column),
        primary: column.column.id === pipeline().primaryColumnId,
      })),
    snapshot: () => {
      if (!queryReadyGate(rows) || !queryReadyGate(table)) return;
      const records: DatabaseRow[] = rows.data.records.map((row) => ({
        rowId: row.rowId,
        cells: Object.fromEntries(
          columns().map((column) => [
            column.column.id,
            pipelineCell(row.cells[column.column.id], column),
          ])
        ),
      }));
      records.sort((a, b) => {
        for (const key of view().query.sort ?? []) {
          const left = a.cells[key.column],
            right = b.cells[key.column];
          if (left === right) continue;
          if (left == null) return 1;
          if (right == null) return -1;
          const order =
            typeof left === 'number' && typeof right === 'number'
              ? left - right
              : String(left).localeCompare(String(right));
          if (order) return key.direction === 'descending' ? -order : order;
        }
        return 0;
      });
      return { rows: records, retained: records, version: rows.data.version };
    },
    read: () => undefined,
    loading: () => table.isPending || rows.isPending,
    refreshing: () => table.isRefetching || rows.isRefetching,
    error: () =>
      table.isError || rows.isError
        ? { kind: 'fetch', message: 'Could not load pipeline' }
        : undefined,
    refresh: () =>
      ResultAsync.fromPromise(refresh(), () => ({
        kind: 'fetch' as const,
        message: 'Could not refresh pipeline',
      })),
    retain: () => {},
    addOption,
    write: (mutation, _inferenceVersion, createOptions) => {
      const op = mutationOp(tableId, mutation, (id) => {
        const column = columns().find((column) => column.column.id === id);
        return column?.writable
          ? ok(column)
          : err({ kind: 'read-only-column' as const });
      });
      if (op.isErr()) return errAsync(op.error);
      const options: DatabaseOp[] = createOptions
        ? missingOptionLabels(
            op.value,
            (id) =>
              source
                .columns()
                .find((column) => column.id === id)
                ?.options.map((option) => option.label) ?? []
          ).map(({ column, labels }) => ({
            kind: 'column',
            table: tableId,
            column,
            change: {
              kind: 'add_options',
              options: labels.map((label) => ({ id: uuidv7(), label })),
            },
          }))
        : [];
      return apply([...options, op.value])
        .mapErr((error): DatabaseWriteFailure => ({ kind: 'ops', error }))
        .map((applied) => ({
          insertedRowIds: applied.results.flatMap((result) =>
            result.kind === 'rows' && result.change.kind === 'inserted'
              ? result.change.rows
              : []
          ),
          version: applied.results.find((result) => result.kind === 'rows')
            ?.tableVersion,
        }));
    },
  };
  const changeColumn = (
    column: string,
    change: Extract<DatabaseOp, { kind: 'column' }>['change'],
    version?: number
  ) =>
    apply([{ kind: 'column', table: tableId, column, change }], version).map(
      () => undefined
    );
  const optionEditing: OptionEditing = {
    update: (column, option, change) =>
      changeColumn(column, { kind: 'update_option', option, ...change }).mapErr(
        (error) => ({ kind: 'ops', error })
      ),
    remove: (column, option) =>
      changeColumn(column, { kind: 'delete_option', option }).mapErr(
        (error) => ({ kind: 'ops', error })
      ),
  };
  return {
    source,
    optionEditing,
    createColumn: () => {
      const column = uuidv7();
      return changeColumn(column, {
        kind: 'create',
        definition: {
          source: 'new',
          name: defaultDatabaseColumnName(
            source.columns().map((column) => column.name)
          ),
          type: { type: 'text' },
        },
      }).map(() => column);
    },
    renameColumn: (column: string, name: string, previousName: string) =>
      changeColumn(column, { kind: 'rename', name, previousName }),
    deleteColumn: (column: string) => changeColumn(column, { kind: 'delete' }),
    changeColumnType: (column: string, change: DatabaseColumnTypeChange) =>
      changeColumn(
        column,
        {
          kind: 'change_type',
          to: opColumnKind(pipeline().databaseId, change.to),
        },
        change.baseVersion
      ),
    reorderColumns: (columns: string[]) =>
      apply([
        {
          kind: 'table',
          table: tableId,
          change: { kind: 'reorder_columns', order: columns },
        },
      ]).map(() => undefined),
  };
}
