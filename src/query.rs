use crate::protocol::IgniteValue;

/// A named column in a query result set.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

/// A single row returned from a query.
#[derive(Debug, Clone)]
pub struct Row {
    pub(crate) columns: Vec<Column>,
    pub(crate) values: Vec<IgniteValue>,
}

impl Row {
    pub(crate) fn new(columns: Vec<Column>, values: Vec<IgniteValue>) -> Self {
        Self { columns, values }
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value by column index.
    pub fn get(&self, index: usize) -> Option<&IgniteValue> {
        self.values.get(index)
    }

    /// Get a value by column name (case-insensitive).
    pub fn get_by_name(&self, name: &str) -> Option<&IgniteValue> {
        let name_upper = name.to_uppercase();
        self.columns
            .iter()
            .position(|c| c.name.to_uppercase() == name_upper)
            .and_then(|i| self.values.get(i))
    }

    /// Column descriptors in declaration order.  Indices match [`Self::get`].
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// All values in column order.  Indices match [`Self::get`].
    pub fn values(&self) -> &[IgniteValue] {
        &self.values
    }
}

/// The complete result of a SQL SELECT (all pages fetched).
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    /// Returns an empty result set with zero rows and zero columns.
    pub fn empty() -> Self {
        Self { columns: vec![], rows: vec![] }
    }

    /// Number of rows returned.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn first_row(&self) -> Option<&Row> {
        self.rows.first()
    }
}

/// Result of a DML operation (INSERT/UPDATE/DELETE).
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of rows affected (-1 if not returned by server).
    pub rows_affected: i64,
}
