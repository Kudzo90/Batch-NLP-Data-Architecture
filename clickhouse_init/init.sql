-- This script runs automatically on first container start
CREATE TABLE IF NOT EXISTS quarterly_stats (
    year UInt16,
    quarter UInt8,
    category String,
    article_count UInt64,
    avg_sentiment Float32,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (year, quarter)
ORDER BY (year, quarter, category);
