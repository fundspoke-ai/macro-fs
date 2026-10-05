import type { ColumnDetail } from '@service-storage/generated/schemas/columnDetail';
import { AddColumnButton } from '../components/add-column-button';
import { defaultDatabaseColumnName } from '../core/property-creation';
import { createDatabaseColumn } from '../queries/columns';

/**
 * Add a Text column with the next free default name at the table's end; its
 * type is inferred from what is typed. Returns the new column's id.
 */
export function createDefaultColumn(args: {
  databaseId: string;
  tableId: string;
  columns: ColumnDetail[];
}) {
  const name = defaultDatabaseColumnName(
    args.columns.map(
      (entry) =>
        entry.column.display_name ?? entry.definition.definition.display_name
    )
  );
  return createDatabaseColumn({
    databaseId: args.databaseId,
    tableId: args.tableId,
    name,
    type: { type: 'text' },
    inferType: true,
  });
}

/** A new column starts as Text; name and type are edited in its header. */
export function AddColumnMenu(props: {
  databaseId: string;
  tableId: string;
  columns: ColumnDetail[];
  label?: string;
  onCreated?: (columnId: string) => boolean;
}) {
  return (
    <AddColumnButton {...props} create={() => createDefaultColumn(props)} />
  );
}
