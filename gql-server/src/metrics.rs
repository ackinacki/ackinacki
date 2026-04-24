// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::ObservableGauge;
use opentelemetry::KeyValue;
use telemetry_utils::get_metrics_endpoint;
use telemetry_utils::init_meter_provider;
use telemetry_utils::TokioMetrics;

use crate::schema::db::DBConnector;

#[derive(Clone)]
pub struct GqlServerMetrics {
    build_info: Gauge<u64>,
    query_duration: Histogram<u64>,
    query_errors: Counter<u64>,
    pub(crate) sqlite_query_duration: Histogram<u64>,
    _pool_size: ObservableGauge<u64>,
    _pool_idle: ObservableGauge<u64>,
}

#[derive(Clone)]
pub struct Metrics {
    pub gql: GqlServerMetrics,
    pub _tokio: TokioMetrics,
    pub endpoint: String,
}

pub const DEFAULT_QUERY_DURATION_BOUNDARIES: &[f64] = &[
    5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0,
    60000.0,
];

pub const DEFAULT_SQLITE_QUERY_BOUNDARIES: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
    30000.0, 60000.0,
];

impl Metrics {
    pub fn new(
        db_connector: Arc<DBConnector>,
        query_duration_boundaries: Option<Vec<f64>>,
        sqlite_query_boundaries: Option<Vec<f64>>,
    ) -> Option<Self> {
        let endpoint = get_metrics_endpoint()?;
        opentelemetry::global::set_meter_provider(init_meter_provider());
        let meter = opentelemetry::global::meter("gql-server");
        let metrics = Metrics {
            gql: GqlServerMetrics::new(
                &meter,
                db_connector,
                query_duration_boundaries,
                sqlite_query_boundaries,
            ),
            _tokio: TokioMetrics::new(&meter),
            endpoint,
        };
        Some(metrics)
    }
}

impl GqlServerMetrics {
    pub fn new(
        meter: &Meter,
        db_connector: Arc<DBConnector>,
        query_duration_boundaries: Option<Vec<f64>>,
        sqlite_query_boundaries: Option<Vec<f64>>,
    ) -> Self {
        let connector_size = Arc::clone(&db_connector);
        let connector_idle = db_connector;

        let query_duration_boundaries =
            query_duration_boundaries.unwrap_or_else(|| DEFAULT_QUERY_DURATION_BOUNDARIES.to_vec());
        let sqlite_query_boundaries =
            sqlite_query_boundaries.unwrap_or_else(|| DEFAULT_SQLITE_QUERY_BOUNDARIES.to_vec());

        Self {
            build_info: meter.u64_gauge("gql_build_info").build(),

            query_duration: meter
                .u64_histogram("gql_query_duration")
                .with_description("Successful GraphQL query execution duration in milliseconds")
                .with_boundaries(query_duration_boundaries)
                .build(),

            query_errors: meter
                .u64_counter("gql_query_errors_total")
                .with_description("Total number of GraphQL queries that returned errors")
                .build(),

            sqlite_query_duration: meter
                .u64_histogram("gql_sqlite_query_duration")
                .with_description("SQLite query execution duration in milliseconds")
                .with_boundaries(sqlite_query_boundaries)
                .build(),

            _pool_size: meter
                .u64_observable_gauge("gql_sqlite_pool_size")
                .with_description("Total number of connections in the SQLite pool")
                .with_callback(move |observer| {
                    observer.observe(connector_size.pool_size() as u64, &[]);
                })
                .build(),

            _pool_idle: meter
                .u64_observable_gauge("gql_sqlite_pool_idle")
                .with_description("Number of idle connections in the SQLite pool")
                .with_callback(move |observer| {
                    observer.observe(connector_idle.pool_idle() as u64, &[]);
                })
                .build(),
        }
    }

    pub fn report_query_success(&self, duration_ms: u64) {
        self.query_duration.record(duration_ms, &[]);
    }

    pub fn report_query_error(&self) {
        self.query_errors.add(1, &[]);
    }

    pub fn report_build_info(&self) {
        let version = env!("CARGO_PKG_VERSION");
        let commit = env!("BUILD_GIT_COMMIT");
        self.build_info
            .record(1, &[KeyValue::new("version", version), KeyValue::new("commit", commit)]);
    }
}
