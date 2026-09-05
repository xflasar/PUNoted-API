-- ============================================================================
-- PostgreSQL Table & Index Schema: shipyards
-- ============================================================================

CREATE TABLE IF NOT EXISTS shipyards (
    activeprojectstotal INTEGER,
    createdprojectstotal INTEGER,
    currencyid TEXT,
    finishedprojectsmonth INTEGER,
    finishedprojectssemiannually INTEGER,
    finishedprojectstotal INTEGER,
    finishedprojectsweek INTEGER,
    id TEXT NOT NULL,
    operatortype TEXT,
    planetid TEXT,
    systemid TEXT,
    PRIMARY KEY (id)
);
