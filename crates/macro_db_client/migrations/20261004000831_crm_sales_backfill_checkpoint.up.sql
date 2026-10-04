-- One completion marker per CRM. Keep the marker after pipeline deletion so
-- retrying the operator backfill never resurrects something a user removed.
CREATE TABLE crm_sales_backfills (
    team_id UUID PRIMARY KEY REFERENCES team(id) ON DELETE CASCADE,
    pipeline_id UUID REFERENCES crm_pipelines(id) ON DELETE SET NULL,
    company_count BIGINT NOT NULL CHECK (company_count >= 0),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
