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
    MAX(PENDING) - MIN(PENDING) AS pending_increase,
    -- Calculate pending-to-running ratio (higher ratio indicates potential bottleneck)
    CASE 
        WHEN AVG(CAST(RUNNING AS FLOAT)) = 0 THEN NULL
        ELSE AVG(CAST(PENDING AS FLOAT)) / AVG(CAST(RUNNING AS FLOAT))
    END AS pending_to_running_ratio,
    -- Calculate rate of pending queue reduction (negative value means queue is reducing)
    CASE
        WHEN DATEDIFF(SECOND, MIN(PATTERN_TIME), MAX(PATTERN_TIME)) = 0 THEN NULL
        ELSE (MIN(PENDING) - MAX(PENDING)) / CAST(DATEDIFF(SECOND, MIN(PATTERN_TIME), MAX(PATTERN_TIME)) AS FLOAT)
    END AS pending_reduction_rate_per_second
FROM thread_monitoring
GROUP BY 
    FORMAT(PATTERN_TIME, 'yyyy-MM-dd HH:mm:00'),
    DATASET,
    APP_JVM,
    report_type
HAVING 
    MAX(PENDING) - MIN(PENDING) <> 0 -- Show any change in pending queue
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

-- NEW QUERY: Identify datasets with highest average load
SELECT TOP 10
    DATASET,
    report_type,
    AVG(CAST(RUNNING AS FLOAT)) AS avg_running,
    AVG(CAST(PENDING AS FLOAT)) AS avg_pending,
    -- Calculate average processing time indicator (pending-to-running ratio)
    AVG(CAST(PENDING AS FLOAT)) / NULLIF(AVG(CAST(RUNNING AS FLOAT)), 0) AS avg_processing_time_indicator,
    -- Calculate max pending count
    MAX(PENDING) AS max_pending_count,
    -- Calculate total samples
    COUNT(*) AS sample_count
FROM thread_monitoring
WHERE 
    -- Filter for the past 5 days excluding weekends (Saturday and Sunday)
    PATTERN_TIME >= DATEADD(DAY, -5, GETDATE())
    AND DATEPART(WEEKDAY, PATTERN_TIME) NOT IN (1, 7)  -- 1=Sunday, 7=Saturday in DATEPART
GROUP BY
    DATASET,
    report_type
ORDER BY
    avg_pending DESC;

-- NEW QUERY: Identify nodes with highest average load
SELECT TOP 10
    APP_JVM AS node,
    report_type,
    AVG(CAST(RUNNING AS FLOAT)) AS avg_running,
    AVG(CAST(PENDING AS FLOAT)) AS avg_pending,
    -- Calculate average processing time indicator (pending-to-running ratio)
    AVG(CAST(PENDING AS FLOAT)) / NULLIF(AVG(CAST(RUNNING AS FLOAT)), 0) AS avg_processing_time_indicator,
    -- Calculate max pending count
    MAX(PENDING) AS max_pending_count,
    -- Calculate total samples
    COUNT(*) AS sample_count
FROM thread_monitoring
WHERE 
    -- Filter for the past 5 days excluding weekends (Saturday and Sunday)
    PATTERN_TIME >= DATEADD(DAY, -5, GETDATE())
    AND DATEPART(WEEKDAY, PATTERN_TIME) NOT IN (1, 7)  -- 1=Sunday, 7=Saturday in DATEPART
GROUP BY
    APP_JVM,
    report_type
ORDER BY
    avg_pending DESC;

-- NEW QUERY: Calculate processing efficiency by dataset and node
-- Lower pending reduction rate indicates slower processing
SELECT
    DATASET,
    APP_JVM AS node,
    report_type,
    -- Calculate average pending reduction rate per minute
    AVG(
        CASE
            WHEN DATEDIFF(MINUTE, LAG(PATTERN_TIME) OVER (PARTITION BY DATASET, APP_JVM, report_type ORDER BY PATTERN_TIME), PATTERN_TIME) = 0 THEN NULL
            ELSE (LAG(PENDING) OVER (PARTITION BY DATASET, APP_JVM, report_type ORDER BY PATTERN_TIME) - PENDING) / 
                 CAST(DATEDIFF(MINUTE, LAG(PATTERN_TIME) OVER (PARTITION BY DATASET, APP_JVM, report_type ORDER BY PATTERN_TIME), PATTERN_TIME) AS FLOAT)
        END
    ) AS avg_pending_reduction_rate_per_minute,
    -- Calculate average running threads
    AVG(CAST(RUNNING AS FLOAT)) AS avg_running,
    -- Calculate average pending count
    AVG(CAST(PENDING AS FLOAT)) AS avg_pending,
    -- Calculate max pending count
    MAX(PENDING) AS max_pending
FROM thread_monitoring
WHERE 
    -- Filter for the past 5 days excluding weekends (Saturday and Sunday)
    PATTERN_TIME >= DATEADD(DAY, -5, GETDATE())
    AND DATEPART(WEEKDAY, PATTERN_TIME) NOT IN (1, 7)  -- 1=Sunday, 7=Saturday in DATEPART
GROUP BY
    DATASET,
    APP_JVM,
    report_type
HAVING
    COUNT(*) > 1 -- Need at least 2 samples to calculate reduction rate
ORDER BY
    avg_pending_reduction_rate_per_minute ASC; -- Ascending order to show slowest processing first

-- Variable to control the number of days to analyze
DECLARE @DaysToAnalyze INT = 5; -- Change this value to analyze more or fewer days

-- NEW QUERY: Show last N days comparison with one column per day using PIVOT
WITH daily_metrics AS (
    SELECT
        CAST(FORMAT(PATTERN_TIME, 'yyyy-MM-dd') AS DATE) AS date_only,
        DATASET,
        APP_JVM AS node,
        report_type,
        AVG(CAST(RUNNING AS FLOAT)) AS avg_running,
        AVG(CAST(PENDING AS FLOAT)) AS avg_pending,
        MAX(RUNNING) AS max_running,
        MAX(PENDING) AS max_pending,
        COUNT(*) AS sample_count
    FROM thread_monitoring
    WHERE 
        -- Filter for the past N days excluding weekends (Saturday and Sunday)
        PATTERN_TIME >= DATEADD(DAY, -@DaysToAnalyze, GETDATE())
        AND DATEPART(WEEKDAY, PATTERN_TIME) NOT IN (1, 7)  -- 1=Sunday, 7=Saturday in DATEPART
    GROUP BY
        CAST(FORMAT(PATTERN_TIME, 'yyyy-MM-dd') AS DATE),
        DATASET,
        APP_JVM,
        report_type
),
days_list AS (
    SELECT 
        date_only,
        'day_' + CAST(ROW_NUMBER() OVER (ORDER BY date_only DESC) AS VARCHAR(2)) AS day_column
    FROM (SELECT DISTINCT date_only FROM daily_metrics) AS distinct_dates
    ORDER BY date_only DESC
),
-- Prepare data for pivoting with metrics and day columns
metrics_for_pivot AS (
    SELECT
        m.DATASET,
        m.node,
        m.report_type,
        d.day_column,
        'avg_running' AS metric_type,
        m.avg_running AS metric_value
    FROM daily_metrics m
    JOIN days_list d ON m.date_only = d.date_only
    
    UNION ALL
    
    SELECT
        m.DATASET,
        m.node,
        m.report_type,
        d.day_column,
        'avg_pending' AS metric_type,
        m.avg_pending AS metric_value
    FROM daily_metrics m
    JOIN days_list d ON m.date_only = d.date_only
    
    UNION ALL
    
    SELECT
        m.DATASET,
        m.node,
        m.report_type,
        d.day_column,
        'max_running' AS metric_type,
        m.max_running AS metric_value
    FROM daily_metrics m
    JOIN days_list d ON m.date_only = d.date_only
    
    UNION ALL
    
    SELECT
        m.DATASET,
        m.node,
        m.report_type,
        d.day_column,
        'max_pending' AS metric_type,
        m.max_pending AS metric_value
    FROM daily_metrics m
    JOIN days_list d ON m.date_only = d.date_only
),
-- Use a static PIVOT with conditional filtering based on @DaysToAnalyze
WITH filtered_metrics AS (
    SELECT
        m.DATASET,
        m.node,
        m.report_type,
        m.metric_type,
        m.day_column,
        m.metric_value
    FROM metrics_for_pivot m
    WHERE CAST(SUBSTRING(m.day_column, 5, 2) AS INT) <= @DaysToAnalyze
),
pivoted_metrics AS (
    SELECT * FROM filtered_metrics
    PIVOT (
        MAX(metric_value)
        FOR day_column IN ([day_1], [day_2], [day_3], [day_4], [day_5], 
                          [day_6], [day_7], [day_8], [day_9], [day_10])
    ) AS pivot_table
),
final_metrics AS (
    SELECT
        DATASET,
        node,
        report_type,
        -- Day 1 (Most recent day)
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_1] END) AS day1_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_1] END) AS day1_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_1] END) AS day1_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_1] END) AS day1_max_pending,
        
        -- Day 2
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_2] END) AS day2_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_2] END) AS day2_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_2] END) AS day2_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_2] END) AS day2_max_pending,
        
        -- Day 3
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_3] END) AS day3_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_3] END) AS day3_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_3] END) AS day3_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_3] END) AS day3_max_pending,
        
        -- Day 4
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_4] END) AS day4_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_4] END) AS day4_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_4] END) AS day4_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_4] END) AS day4_max_pending,
        
        -- Day 5
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_5] END) AS day5_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_5] END) AS day5_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_5] END) AS day5_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_5] END) AS day5_max_pending,
        
        -- Additional days (will be NULL if @DaysToAnalyze < 6)
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_6] END) AS day6_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_6] END) AS day6_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_6] END) AS day6_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_6] END) AS day6_max_pending,
        
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_7] END) AS day7_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_7] END) AS day7_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_7] END) AS day7_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_7] END) AS day7_max_pending,
        
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_8] END) AS day8_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_8] END) AS day8_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_8] END) AS day8_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_8] END) AS day8_max_pending,
        
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_9] END) AS day9_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_9] END) AS day9_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_9] END) AS day9_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_9] END) AS day9_max_pending,
        
        MAX(CASE WHEN metric_type = 'avg_running' THEN [day_10] END) AS day10_avg_running,
        MAX(CASE WHEN metric_type = 'avg_pending' THEN [day_10] END) AS day10_avg_pending,
        MAX(CASE WHEN metric_type = 'max_running' THEN [day_10] END) AS day10_max_running,
        MAX(CASE WHEN metric_type = 'max_pending' THEN [day_10] END) AS day10_max_pending,
        
        -- Get the last day number for change calculations based on @DaysToAnalyze
        CASE
            WHEN @DaysToAnalyze >= 10 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_10] END) IS NOT NULL THEN 10
            WHEN @DaysToAnalyze >= 9 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_9] END) IS NOT NULL THEN 9
            WHEN @DaysToAnalyze >= 8 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_8] END) IS NOT NULL THEN 8
            WHEN @DaysToAnalyze >= 7 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_7] END) IS NOT NULL THEN 7
            WHEN @DaysToAnalyze >= 6 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_6] END) IS NOT NULL THEN 6
            WHEN @DaysToAnalyze >= 5 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_5] END) IS NOT NULL THEN 5
            WHEN @DaysToAnalyze >= 4 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_4] END) IS NOT NULL THEN 4
            WHEN @DaysToAnalyze >= 3 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_3] END) IS NOT NULL THEN 3
            WHEN @DaysToAnalyze >= 2 AND MAX(CASE WHEN metric_type = 'avg_running' THEN [day_2] END) IS NOT NULL THEN 2
            WHEN MAX(CASE WHEN metric_type = 'avg_running' THEN [day_1] END) IS NOT NULL THEN 1
            ELSE NULL
        END AS last_day_number
    FROM pivoted_metrics
    GROUP BY
        DATASET,
        node,
        report_type
)
SELECT
    fm.*,
    -- Overall change (day1 - lastDay) - dynamically selects the last available day
    day1_avg_running - 
    CASE fm.last_day_number
        WHEN 10 THEN day10_avg_running
        WHEN 9 THEN day9_avg_running
        WHEN 8 THEN day8_avg_running
        WHEN 7 THEN day7_avg_running
        WHEN 6 THEN day6_avg_running
        WHEN 5 THEN day5_avg_running
        WHEN 4 THEN day4_avg_running
        WHEN 3 THEN day3_avg_running
        WHEN 2 THEN day2_avg_running
        ELSE NULL
    END AS running_change,
    
    day1_avg_pending - 
    CASE fm.last_day_number
        WHEN 10 THEN day10_avg_pending
        WHEN 9 THEN day9_avg_pending
        WHEN 8 THEN day8_avg_pending
        WHEN 7 THEN day7_avg_pending
        WHEN 6 THEN day6_avg_pending
        WHEN 5 THEN day5_avg_pending
        WHEN 4 THEN day4_avg_pending
        WHEN 3 THEN day3_avg_pending
        WHEN 2 THEN day2_avg_pending
        ELSE NULL
    END AS pending_change,
    
    -- Percent change - dynamically selects the last available day
    CASE
        WHEN fm.last_day_number = 10 AND day10_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 9 AND day9_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 8 AND day8_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 7 AND day7_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 6 AND day6_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 5 AND day5_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 4 AND day4_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 3 AND day3_avg_running = 0 THEN NULL
        WHEN fm.last_day_number = 2 AND day2_avg_running = 0 THEN NULL
        WHEN fm.last_day_number IS NULL THEN NULL
        ELSE (day1_avg_running - 
            CASE fm.last_day_number
                WHEN 10 THEN day10_avg_running
                WHEN 9 THEN day9_avg_running
                WHEN 8 THEN day8_avg_running
                WHEN 7 THEN day7_avg_running
                WHEN 6 THEN day6_avg_running
                WHEN 5 THEN day5_avg_running
                WHEN 4 THEN day4_avg_running
                WHEN 3 THEN day3_avg_running
                WHEN 2 THEN day2_avg_running
                ELSE NULL
            END) / 
            CASE fm.last_day_number
                WHEN 10 THEN day10_avg_running
                WHEN 9 THEN day9_avg_running
                WHEN 8 THEN day8_avg_running
                WHEN 7 THEN day7_avg_running
                WHEN 6 THEN day6_avg_running
                WHEN 5 THEN day5_avg_running
                WHEN 4 THEN day4_avg_running
                WHEN 3 THEN day3_avg_running
                WHEN 2 THEN day2_avg_running
                ELSE NULL
            END * 100
    END AS running_pct_change,
    
    CASE 
        WHEN fm.last_day_number = 10 AND day10_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 9 AND day9_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 8 AND day8_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 7 AND day7_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 6 AND day6_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 5 AND day5_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 4 AND day4_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 3 AND day3_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number = 2 AND day2_avg_pending = 0 THEN NULL
        WHEN fm.last_day_number IS NULL THEN NULL
        ELSE (day1_avg_pending - 
            CASE fm.last_day_number
                WHEN 10 THEN day10_avg_pending
                WHEN 9 THEN day9_avg_pending
                WHEN 8 THEN day8_avg_pending
                WHEN 7 THEN day7_avg_pending
                WHEN 6 THEN day6_avg_pending
                WHEN 5 THEN day5_avg_pending
                WHEN 4 THEN day4_avg_pending
                WHEN 3 THEN day3_avg_pending
                WHEN 2 THEN day2_avg_pending
                ELSE NULL
            END) / 
            CASE fm.last_day_number
                WHEN 10 THEN day10_avg_pending
                WHEN 9 THEN day9_avg_pending
                WHEN 8 THEN day8_avg_pending
                WHEN 7 THEN day7_avg_pending
                WHEN 6 THEN day6_avg_pending
                WHEN 5 THEN day5_avg_pending
                WHEN 4 THEN day4_avg_pending
                WHEN 3 THEN day3_avg_pending
                WHEN 2 THEN day2_avg_pending
                ELSE NULL
            END * 100
    END AS pending_pct_change
FROM final_metrics
ORDER BY
    day1_avg_pending DESC;

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
