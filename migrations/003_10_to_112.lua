return {
  postgres = {
    up = [[
      CREATE INDEX IF NOT EXISTS datalimiting_metrics_idx
      ON response_datalimiting_metrics (service_id, route_id, period_date, period);
    ]],
  },

  cassandra = {
    up = [[ ]],
  },
}
