-- Migration: Add roll strategy to contract_series names
-- This ensures each combination of (root + rank + roll_strategy) is unique
-- 
-- Example:
--   ES_FRONT_MONTH -> ES_FRONT_CALENDAR_2D
--   NQ_FRONT_MONTH -> NQ_FRONT_CALENDAR_2D

-- Update continuous contract definitions
UPDATE dim_continuous_contract
SET 
    contract_series = CASE 
        WHEN contract_series = 'ES_FRONT_MONTH' THEN 'ES_FRONT_CALENDAR_2D'
        WHEN contract_series = 'NQ_FRONT_MONTH' THEN 'NQ_FRONT_CALENDAR_2D'
        ELSE contract_series  -- Keep any other series as-is
    END,
    description = CASE
        WHEN contract_series = 'ES_FRONT_MONTH' THEN 'ES continuous front month (roll: 2-day pre-expiry calendar)'
        WHEN contract_series = 'NQ_FRONT_MONTH' THEN 'NQ continuous front month (roll: 2-day pre-expiry calendar)'
        ELSE description  -- Keep any other descriptions as-is
    END
WHERE contract_series IN ('ES_FRONT_MONTH', 'NQ_FRONT_MONTH');

-- Update all fact tables that reference the old contract_series
UPDATE g_continuous_bar_daily
SET contract_series = CASE 
    WHEN contract_series = 'ES_FRONT_MONTH' THEN 'ES_FRONT_CALENDAR_2D'
    WHEN contract_series = 'NQ_FRONT_MONTH' THEN 'NQ_FRONT_CALENDAR_2D'
    ELSE contract_series
END
WHERE contract_series IN ('ES_FRONT_MONTH', 'NQ_FRONT_MONTH');

UPDATE g_continuous_bar_1m
SET contract_series = CASE 
    WHEN contract_series = 'ES_FRONT_MONTH' THEN 'ES_FRONT_CALENDAR_2D'
    WHEN contract_series = 'NQ_FRONT_MONTH' THEN 'NQ_FRONT_CALENDAR_2D'
    ELSE contract_series
END
WHERE contract_series IN ('ES_FRONT_MONTH', 'NQ_FRONT_MONTH');

UPDATE f_continuous_quote_l1
SET contract_series = CASE 
    WHEN contract_series = 'ES_FRONT_MONTH' THEN 'ES_FRONT_CALENDAR_2D'
    WHEN contract_series = 'NQ_FRONT_MONTH' THEN 'NQ_FRONT_CALENDAR_2D'
    ELSE contract_series
END
WHERE contract_series IN ('ES_FRONT_MONTH', 'NQ_FRONT_MONTH');

UPDATE f_continuous_trade
SET contract_series = CASE 
    WHEN contract_series = 'ES_FRONT_MONTH' THEN 'ES_FRONT_CALENDAR_2D'
    WHEN contract_series = 'NQ_FRONT_MONTH' THEN 'NQ_FRONT_CALENDAR_2D'
    ELSE contract_series
END
WHERE contract_series IN ('ES_FRONT_MONTH', 'NQ_FRONT_MONTH');

-- Verify the changes
SELECT 
    contract_series,
    root,
    roll_rule,
    adjustment_method,
    description
FROM dim_continuous_contract
ORDER BY root, contract_series;

