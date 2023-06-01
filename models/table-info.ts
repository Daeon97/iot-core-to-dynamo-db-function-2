export type TableType = "primary" | "secondary";

export interface Table {
    type: TableType
    name: string
}

export class TableInfo {
    constructor(public table: Table) { }
}