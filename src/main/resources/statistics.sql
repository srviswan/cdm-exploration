-- Table definition for thread monitoring data
CREATE TABLE thread_monitoring (
    PATTERN_TIME DATETIME,
    MAX_THREADS INT,
    RUNNING INT,
    PENDING INT,
    DATASET VARCHAR(255),
    APP_JVM VARCHAR(255),
    report_type VARCHAR(50)
);

-- Sample data insertion (for testing purposes)
-- INSERT INTO thread_monitoring VALUES
--   ('2025-03-15 10:00:00', 100, 45, 12, 'dataset1', 'node1', 'daily'),
--   ('2025-03-15 10:00:05', 100, 48, 15, 'dataset1', 'node2', 'weekly'),
--   ('2025-03-15 10:00:10', 100, 50, 18, 'dataset2', 'node1', 'daily'),
--   ('2025-03-15 10:00:15', 100, 52, 20, 'dataset2', 'node2', 'monthly');

-- Query to calculate per minute run rate and pending count build up
-- This query groups data by minute, dataset, and node to provide aggregated metrics
SELECT 
    FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00') AS minute_bucket,
    DATASET,
    APP_JVM AS node,
    report_type,
    -- Average values within each minute
    AVG(CAST(RUNNING AS FLOAT)) AS avg_running,
    AVG(CAST(PENDING AS FLOAT)) AS avg_pending,
    -- Min and max values within each minute
    MIN(RUNNING) AS min_running,
    MAX(RUNNING) AS max_running,
    MIN(PENDING) AS min_pending,
    MAX(PENDING) AS max_pending,
    -- Calculate the rate of change within each minute
    (MAX(RUNNING) - MIN(RUNNING)) AS running_change_per_minute,
    (MAX(PENDING) - MIN(PENDING)) AS pending_change_per_minute,
    -- Count of samples in this minute
    COUNT(*) AS sample_count
FROM thread_monitoring
GROUP BY 
    FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00'),
    DATASET,
    APP_JVM,
    report_type
ORDER BY 
    minute_bucket,
    DATASET,
    APP_JVM,
    report_type;

-- Alternative query that calculates the rate by comparing to the previous minute
WITH minute_stats AS (
    SELECT 
        FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00') AS minute_bucket,
        DATASET,
        APP_JVM AS node,
        report_type,
        AVG(CAST(RUNNING AS FLOAT)) AS avg_running,
        AVG(CAST(PENDING AS FLOAT)) AS avg_pending
    FROM thread_monitoring
    GROUP BY 
        FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00'),
        DATASET,
        APP_JVM
)
SELECT 
    current.minute_bucket,
    current.DATASET,
    current.node,
    current.avg_running,
    current.avg_pending,
    -- Calculate change from previous minute
    (current.avg_running - ISNULL(prev.avg_running, 0)) AS running_change_from_prev_minute,
    (current.avg_pending - ISNULL(prev.avg_pending, 0)) AS pending_change_from_prev_minute
FROM 
    minute_stats current
LEFT JOIN 
    minute_stats prev ON 
    current.DATASET = prev.DATASET AND
    current.node = prev.node AND
    current.report_type = prev.report_type AND
    prev.minute_bucket = FORMAT(DATEADD(MINUTE, -1, CONVERT(DATETIME, current.minute_bucket, 120)), 'yyyy-MM-dd HH:mm:00')
ORDER BY 
    current.minute_bucket,
    current.DATASET,
    current.node,
    current.report_type;

-- Query to identify datasets with increasing pending counts (potential bottlenecks)
SELECT 
    FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00') AS minute_bucket,
    DATASET,
    APP_JVM AS node,
    report_type,
    AVG(CAST(PENDING AS FLOAT)) AS avg_pending,
    MAX(PENDING) - MIN(PENDING) AS pending_increase
FROM thread_monitoring
GROUP BY 
    FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00'),
    DATASET,
    APP_JVM,
    report_type
HAVING 
    MAX(PENDING) - MIN(PENDING) > 0
ORDER BY 
    pending_increase DESC,
    minute_bucket;

-- Query to get total system throughput per minute (across all datasets and nodes)
SELECT 
    FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00') AS minute_bucket,
    report_type,
    SUM(CAST(RUNNING AS FLOAT)) / COUNT(DISTINCT APP_JVM) AS avg_total_running,
    SUM(CAST(PENDING AS FLOAT)) / COUNT(DISTINCT APP_JVM) AS avg_total_pending,
    COUNT(DISTINCT DATASET) AS dataset_count,
    COUNT(DISTINCT APP_JVM) AS node_count
FROM thread_monitoring
GROUP BY 
    FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00'),
    report_type
ORDER BY 
    minute_bucket,
    report_type;

-- Query to compare metrics for every 15 minutes over the past 5 days
-- This creates 15-minute buckets and compares metrics across datasets and nodes
WITH fifteen_min_buckets AS (
    SELECT 
        -- Create 15-minute time buckets by truncating minutes to 00, 15, 30, 45
        DATEADD(MINUTE, (DATEDIFF(MINUTE, '2000-01-01', PATTERN_TIME) / 15) * 15, '2000-01-01') AS time_bucket,
        FORMAT(DATEADD(MINUTE, (DATEDIFF(MINUTE, '2000-01-01', PATTERN_TIME) / 15) * 15, '2000-01-01'), 'yyyy-MM-dd HH:mm') AS time_bucket_str,
        -- Extract just the time part for comparison across days
        FORMAT(DATEADD(MINUTE, (DATEDIFF(MINUTE, '2000-01-01', PATTERN_TIME) / 15) * 15, '2000-01-01'), 'HH:mm') AS time_of_day,
        CAST(FORMAT(PATTERN_TIME, 'yyyy-MM-dd') AS DATE) AS date_only,
        DATASET,
        APP_JVM AS node,
        report_type,
        RUNNING,
        PENDING
    FROM thread_monitoring
    WHERE 
        -- Filter for the past 5 days excluding weekends (Saturday and Sunday)
        PATTERN_TIME >= DATEADD(DAY, -5, GETDATE())
        AND DATEPART(WEEKDAY, PATTERN_TIME) NOT IN (1, 7)  -- 1=Sunday, 7=Saturday in DATEPART
),
summary_stats AS (
    SELECT
        time_of_day,
        date_only,
        DATASET,
        node,
        report_type,
        AVG(CAST(RUNNING AS FLOAT)) AS avg_running,
        AVG(CAST(PENDING AS FLOAT)) AS avg_pending,
        MAX(RUNNING) - MIN(RUNNING) AS running_change,
        MAX(PENDING) - MIN(PENDING) AS pending_change,
        COUNT(*) AS sample_count
    FROM fifteen_min_buckets
    GROUP BY
        time_of_day,
        date_only,
        DATASET,
        node,
        report_type
)
SELECT
    s.time_of_day,
    s.date_only,
    s.DATASET,
    s.node,
    s.report_type,
    s.avg_running,
    s.avg_pending,
    s.running_change,
    s.pending_change,
    -- Compare with the same time slot on the previous day
    s.avg_running - ISNULL(prev_day.avg_running, 0) AS running_diff_from_prev_day,
    s.avg_pending - ISNULL(prev_day.avg_pending, 0) AS pending_diff_from_prev_day,
    -- Calculate percentage change
    CASE 
        WHEN ISNULL(prev_day.avg_running, 0) = 0 THEN NULL 
        ELSE (s.avg_running - prev_day.avg_running) / prev_day.avg_running * 100 
    END AS running_pct_change,
    CASE 
        WHEN ISNULL(prev_day.avg_pending, 0) = 0 THEN NULL 
        ELSE (s.avg_pending - prev_day.avg_pending) / prev_day.avg_pending * 100 
    END AS pending_pct_change
FROM 
    summary_stats s
LEFT JOIN 
    summary_stats prev_day ON
    s.time_of_day = prev_day.time_of_day AND
    s.DATASET = prev_day.DATASET AND
    s.node = prev_day.node AND
    s.report_type = prev_day.report_type AND
    prev_day.date_only = DATEADD(DAY, -1, s.date_only)
ORDER BY
    s.date_only DESC,
    s.time_of_day,
    s.DATASET,
    s.node,
    s.report_type;

-- Query to identify patterns by time of day (15-minute intervals) across all 5 days
-- This helps identify if certain times of day consistently show higher load
WITH fifteen_min_buckets AS (
    SELECT 
        -- Extract just the time part for comparison across days
        FORMAT(DATEADD(MINUTE, (DATEDIFF(MINUTE, '2000-01-01', PATTERN_TIME) / 15) * 15, '2000-01-01'), 'HH:mm') AS time_of_day,
        DATASET,
        APP_JVM AS node,
        report_type,
        RUNNING,
        PENDING
    FROM thread_monitoring
    WHERE 
        -- Filter for the past 5 days excluding weekends (Saturday and Sunday)
        PATTERN_TIME >= DATEADD(DAY, -5, GETDATE())
        AND DATEPART(WEEKDAY, PATTERN_TIME) NOT IN (1, 7)  -- 1=Sunday, 7=Saturday in DATEPART
)
SELECT
    time_of_day,
    DATASET,
    node,
    report_type,
    AVG(CAST(RUNNING AS FLOAT)) AS avg_running_all_days,
    AVG(CAST(PENDING AS FLOAT)) AS avg_pending_all_days,
    STDEV(CAST(RUNNING AS FLOAT)) AS running_std_dev,
    STDEV(CAST(PENDING AS FLOAT)) AS pending_std_dev,
    MAX(RUNNING) AS max_running,
    MAX(PENDING) AS max_pending,
    COUNT(*) AS sample_count
FROM 
    fifteen_min_buckets
GROUP BY
    time_of_day,
    DATASET,
    node,
    report_type
ORDER BY
    -- Order by average pending count to highlight potential bottleneck times
    avg_pending_all_days DESC,
    time_of_day,
    DATASET,
    node,
    report_type;
