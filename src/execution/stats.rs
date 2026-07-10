use crate::catalog::TableSchema;

use super::analyze::{ColumnStats, TableStats};

const DEFAULT_RANGE_SELECTIVITY: f64 = 0.333;
const DEFAULT_LIKE_SELECTIVITY: f64 = 0.1;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct EqualityJoinEstimate {
    pub rows: usize,
    pub selectivity: f64,
    pub left_ndv: usize,
    pub right_ndv: usize,
}

pub(crate) struct StatsEstimator<'a> {
    schema: &'a TableSchema,
    stats: &'a TableStats,
}

impl<'a> StatsEstimator<'a> {
    pub(crate) fn new(schema: &'a TableSchema, stats: &'a TableStats) -> Self {
        Self { schema, stats }
    }

    pub(crate) fn equality_selectivity(&self, column_idx: usize) -> Option<f64> {
        let column = self.schema.columns.get(column_idx)?;
        if column.is_primary || column.is_unique {
            return Some(Self::one_row_selectivity(self.stats.row_count));
        }

        let column_stats = self.column_stats_for_schema_index(column_idx)?;
        (column_stats.distinct_count > 0)
            .then(|| (1.0 / column_stats.distinct_count as f64).clamp(0.0, 1.0))
    }

    pub(crate) fn equality_rows(&self, column_idx: usize) -> Option<usize> {
        self.equality_selectivity(column_idx)
            .map(|selectivity| Self::selectivity_to_rows(self.stats.row_count, selectivity))
    }

    pub(crate) fn in_list_selectivity(&self, column_idx: usize, list_len: usize) -> Option<f64> {
        let column = self.schema.columns.get(column_idx)?;
        if column.is_primary || column.is_unique {
            return Some(Self::ratio(
                list_len.min(self.stats.row_count),
                self.stats.row_count,
            ));
        }

        let column_stats = self.column_stats_for_schema_index(column_idx)?;
        (column_stats.distinct_count > 0)
            .then(|| (list_len as f64 / column_stats.distinct_count as f64).clamp(0.0, 1.0))
    }

    pub(crate) fn in_list_rows(&self, column_idx: usize, list_len: usize) -> Option<usize> {
        self.in_list_selectivity(column_idx, list_len)
            .map(|selectivity| Self::selectivity_to_rows(self.stats.row_count, selectivity))
    }

    pub(crate) fn range_selectivity(&self, column_idx: usize) -> Option<f64> {
        self.schema
            .columns
            .get(column_idx)
            .map(|_| DEFAULT_RANGE_SELECTIVITY)
    }

    pub(crate) fn like_selectivity(&self, column_idx: usize) -> Option<f64> {
        self.schema
            .columns
            .get(column_idx)
            .map(|_| DEFAULT_LIKE_SELECTIVITY)
    }

    pub(crate) fn null_selectivity(&self, column_idx: usize) -> Option<f64> {
        let column_stats = self.column_stats_for_schema_index(column_idx)?;
        Some(Self::ratio(column_stats.null_count, self.stats.row_count))
    }

    pub(crate) fn null_rows(&self, column_idx: usize) -> Option<usize> {
        let column_stats = self.column_stats_for_schema_index(column_idx)?;
        Some(column_stats.null_count.min(self.stats.row_count))
    }

    pub(crate) fn not_null_selectivity(&self, column_idx: usize) -> Option<f64> {
        let column_stats = self.column_stats_for_schema_index(column_idx)?;
        Some(Self::ratio(
            self.stats.row_count.saturating_sub(column_stats.null_count),
            self.stats.row_count,
        ))
    }

    pub(crate) fn not_null_rows(&self, column_idx: usize) -> Option<usize> {
        let column_stats = self.column_stats_for_schema_index(column_idx)?;
        Some(self.stats.row_count.saturating_sub(column_stats.null_count))
    }

    pub(crate) fn equality_join_estimate(
        left_schema: &'a TableSchema,
        left_stats: &'a TableStats,
        left_col: usize,
        left_rows: usize,
        right_schema: &'a TableSchema,
        right_stats: &'a TableStats,
        right_col: usize,
        right_rows: usize,
    ) -> Option<EqualityJoinEstimate> {
        if left_rows == 0 || right_rows == 0 {
            return Some(EqualityJoinEstimate {
                rows: 0,
                selectivity: 0.0,
                left_ndv: 0,
                right_ndv: 0,
            });
        }

        let left_estimator = Self::new(left_schema, left_stats);
        let right_estimator = Self::new(right_schema, right_stats);
        let left_column_stats = left_estimator.column_stats_for_schema_index(left_col)?;
        let right_column_stats = right_estimator.column_stats_for_schema_index(right_col)?;

        let left_non_null_base = left_stats
            .row_count
            .saturating_sub(left_column_stats.null_count);
        let right_non_null_base = right_stats
            .row_count
            .saturating_sub(right_column_stats.null_count);
        let left_non_null_rows =
            Self::scale_rows(left_rows, left_non_null_base, left_stats.row_count);
        let right_non_null_rows =
            Self::scale_rows(right_rows, right_non_null_base, right_stats.row_count);
        if left_non_null_rows == 0 || right_non_null_rows == 0 {
            return Some(EqualityJoinEstimate {
                rows: 0,
                selectivity: 0.0,
                left_ndv: 0,
                right_ndv: 0,
            });
        }

        let left_ndv = Self::column_ndv(
            left_schema,
            left_stats,
            left_col,
            left_column_stats,
            left_non_null_rows,
        )?;
        let right_ndv = Self::column_ndv(
            right_schema,
            right_stats,
            right_col,
            right_column_stats,
            right_non_null_rows,
        )?;
        let max_ndv = left_ndv.max(right_ndv);
        if max_ndv == 0 {
            return None;
        }

        let product = left_non_null_rows.saturating_mul(right_non_null_rows);
        let rows = ((left_non_null_rows as f64 * right_non_null_rows as f64) / max_ndv as f64)
            .ceil() as usize;
        let rows = rows.clamp(1, product.max(1));
        let total_product = left_rows.saturating_mul(right_rows).max(1);

        Some(EqualityJoinEstimate {
            rows,
            selectivity: (rows as f64 / total_product as f64).clamp(0.0, 1.0),
            left_ndv,
            right_ndv,
        })
    }

    pub(crate) fn selectivity_to_rows(row_count: usize, selectivity: f64) -> usize {
        if row_count == 0 {
            return 0;
        }
        let rows = (row_count as f64 * selectivity.clamp(0.0, 1.0)).ceil() as usize;
        rows.clamp(1, row_count)
    }

    fn one_row_selectivity(row_count: usize) -> f64 {
        if row_count == 0 {
            0.0
        } else {
            1.0 / row_count as f64
        }
    }

    fn ratio(numerator: usize, denominator: usize) -> f64 {
        if denominator == 0 {
            0.0
        } else {
            (numerator as f64 / denominator as f64).clamp(0.0, 1.0)
        }
    }

    fn scale_rows(rows: usize, numerator: usize, denominator: usize) -> usize {
        if rows == 0 || numerator == 0 || denominator == 0 {
            return 0;
        }
        ((rows as f64 * numerator as f64) / denominator as f64)
            .ceil()
            .clamp(1.0, rows as f64) as usize
    }

    fn column_ndv(
        schema: &TableSchema,
        stats: &TableStats,
        index: usize,
        column_stats: &ColumnStats,
        non_null_rows: usize,
    ) -> Option<usize> {
        if non_null_rows == 0 {
            return Some(0);
        }
        let column = schema.columns.get(index)?;
        let ndv = if column.is_primary || column.is_unique {
            stats
                .row_count
                .saturating_sub(column_stats.null_count)
                .max(column_stats.distinct_count)
        } else {
            column_stats.distinct_count
        };
        (ndv > 0).then(|| ndv.min(non_null_rows).max(1))
    }

    fn column_stats_for_schema_index(&self, index: usize) -> Option<&'a ColumnStats> {
        let column_name = self.schema.columns.get(index)?.name.as_str();
        let unqualified = column_name.rsplit('.').next().unwrap_or(column_name);
        self.stats.columns.iter().find(|column| {
            column.name.eq_ignore_ascii_case(column_name)
                || column.name.eq_ignore_ascii_case(unqualified)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, IndexType, TableSchema};
    use crate::common::Value;
    use crate::execution::analyze::{
        ColumnStats, DistinctCountKind, DistinctCountMethod, TableStats,
    };

    fn column(name: &str, is_primary: bool, is_unique: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type: "INTEGER".to_string(),
            is_primary,
            is_indexed: false,
            index_type: IndexType::None,
            default_value: None,
            is_nullable: !is_primary,
            is_unique,
            check_expr: None,
        }
    }

    fn stats() -> TableStats {
        TableStats {
            table_name: "items".to_string(),
            row_count: 100,
            analyzed_rows: 100,
            sampled: false,
            columns: vec![
                ColumnStats {
                    name: "id".to_string(),
                    null_count: 0,
                    distinct_count: 100,
                    distinct_kind: DistinctCountKind::Exact,
                    distinct_method: DistinctCountMethod::ExactSet,
                    min: Some(Value::Integer(1)),
                    max: Some(Value::Integer(100)),
                    most_common_values: Vec::new(),
                    histogram: Vec::new(),
                },
                ColumnStats {
                    name: "category".to_string(),
                    null_count: 10,
                    distinct_count: 5,
                    distinct_kind: DistinctCountKind::Exact,
                    distinct_method: DistinctCountMethod::ExactSet,
                    min: Some(Value::Integer(1)),
                    max: Some(Value::Integer(5)),
                    most_common_values: Vec::new(),
                    histogram: Vec::new(),
                },
            ],
            updated_at_epoch_ms: 42,
        }
    }

    #[test]
    fn estimates_unique_and_non_unique_equality_rows() {
        let schema = TableSchema::new(
            "items".to_string(),
            vec![column("id", true, true), column("category", false, false)],
        );
        let stats = stats();
        let estimator = StatsEstimator::new(&schema, &stats);

        assert_eq!(estimator.equality_rows(0), Some(1));
        assert_eq!(estimator.equality_rows(1), Some(20));
    }

    #[test]
    fn estimates_in_list_and_null_selectivity() {
        let schema = TableSchema::new(
            "items".to_string(),
            vec![column("id", true, true), column("category", false, false)],
        );
        let stats = stats();
        let estimator = StatsEstimator::new(&schema, &stats);

        assert_eq!(estimator.in_list_rows(1, 2), Some(40));
        assert_eq!(estimator.null_rows(1), Some(10));
        assert_eq!(estimator.not_null_rows(1), Some(90));
        assert_eq!(estimator.null_selectivity(1), Some(0.1));
    }

    #[test]
    fn matches_existing_default_range_and_like_selectivity() {
        let schema = TableSchema::new(
            "items".to_string(),
            vec![column("id", true, true), column("category", false, false)],
        );
        let stats = stats();
        let estimator = StatsEstimator::new(&schema, &stats);

        assert_eq!(
            estimator.range_selectivity(1),
            Some(DEFAULT_RANGE_SELECTIVITY)
        );
        assert_eq!(
            estimator.like_selectivity(1),
            Some(DEFAULT_LIKE_SELECTIVITY)
        );
        assert_eq!(StatsEstimator::selectivity_to_rows(100, 0.333), 34);
    }

    #[test]
    fn estimates_unique_equality_join_cardinality() {
        let schema = TableSchema::new(
            "items".to_string(),
            vec![column("id", true, true), column("category", false, false)],
        );
        let stats = stats();

        let estimate = StatsEstimator::equality_join_estimate(
            &schema, &stats, 0, 100, &schema, &stats, 0, 100,
        )
        .expect("join estimate");

        assert_eq!(estimate.rows, 100);
        assert_eq!(estimate.left_ndv, 100);
        assert_eq!(estimate.right_ndv, 100);
        assert_eq!(estimate.selectivity, 0.01);
    }

    #[test]
    fn estimates_many_to_many_equality_join_cardinality_with_nulls() {
        let schema = TableSchema::new(
            "items".to_string(),
            vec![column("id", true, true), column("category", false, false)],
        );
        let stats = stats();

        let estimate = StatsEstimator::equality_join_estimate(
            &schema, &stats, 1, 100, &schema, &stats, 1, 100,
        )
        .expect("join estimate");

        assert_eq!(estimate.rows, 1620);
        assert_eq!(estimate.left_ndv, 5);
        assert_eq!(estimate.right_ndv, 5);
        assert_eq!(estimate.selectivity, 0.162);
    }
}
