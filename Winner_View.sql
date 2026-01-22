WITH all_data AS (
  SELECT *
  FROM `mythical-willow-431913-s4.Test1.Facebook_Final_New`
),
-- Get one creative link per Ad_Name (most recent)
max_creative_links AS (
  SELECT
    Ad_Name,
    MAX(Ad_Creative_Link) AS max_ad_creative_link
  FROM all_data
  WHERE Ad_Creative_Link IS NOT NULL
  GROUP BY Ad_Name
),
-- Base: Get one row per Ad_Name with latest date attributes
base AS (
  SELECT
    Ad_Name,
    ANY_VALUE(Ad_Id) AS Ad_Id,  -- Just pick one Ad_Id for display
    ANY_VALUE(Campaign_Id) AS campaign_id,
    ANY_VALUE(Campaign_Name) AS campaign_name,
    ANY_VALUE(Adset_Id) AS adset_id,
    ANY_VALUE(Adset_Name) AS adset_name,
    ANY_VALUE(CreativeType) AS creative_type,
    ANY_VALUE(Copywriter) AS copywriter,
    MAX(CreativeStrategist) AS CreativeStrategist,
    ANY_VALUE(Actor) AS actor,
    ARRAY_AGG(Editor IGNORE NULLS ORDER BY Date ASC LIMIT 1)[OFFSET(0)] AS editor,
    ANY_VALUE(Week_Tag) AS week_tag,
    ANY_VALUE(Awareness) AS awareness,
    ANY_VALUE(Angle) AS angle,
    ANY_VALUE(Hook) AS hook,
    ANY_VALUE(Product) AS product,
    ANY_VALUE(Brief_Number) AS Brief_Number,
    ARRAY_AGG(Iteration_X_NetNew ORDER BY Date ASC LIMIT 1)[OFFSET(0)] AS iteration_netnew,
    -- 🆕 ADD THUMBNAIL FROM MAIN TABLE
    ANY_VALUE(Creative_Thumbnail_URL_Fixed) AS main_thumbnail,
    ANY_VALUE(Creative_Facebook_URL_Fixed) AS main_facebook_url,
    MAX(Date) AS latest_date
  FROM all_data
  GROUP BY Ad_Name
),
-- Join base with max creative link
base_with_link AS (
  SELECT
    b.*,
    mcl.max_ad_creative_link AS ad_creative_link
  FROM base b
  LEFT JOIN max_creative_links mcl 
    ON mcl.Ad_Name = b.Ad_Name
),
-- 🔗 Get creative thumbnail and Facebook URLs (strictly one per Ad_Name)
creative_links AS (
  SELECT
    `Ad_Name` AS Ad_Name,
    `Creative_thumbnail_URL` AS creative_thumbnail_url,
    `Creative_Facebook_URL` AS creative_facebook_url
  FROM (
    SELECT
      `Ad_Name`,
      `Creative_thumbnail_URL`,
      `Creative_Facebook_URL`,
      ROW_NUMBER() OVER (PARTITION BY `Ad_Name` ORDER BY `Ad_Name`) AS rn
    FROM mythical-willow-431913-s4.Test1.Apache_Thumbnail_ETL
  )
  WHERE rn = 1
),
-- Calculate lifetime metrics (aggregating all Ad_Ids for each Ad_Name)
lifetime_metrics AS (
  SELECT
    Ad_Name,
    Date AS dt,
    SUM(Spend) OVER (PARTITION BY Ad_Name ORDER BY Date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lifetime_spend,
    SUM(Revenue) OVER (PARTITION BY Ad_Name ORDER BY Date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lifetime_revenue,
    SUM(Purchases) OVER (PARTITION BY Ad_Name ORDER BY Date
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lifetime_conversions
  FROM (
    -- Aggregate by Ad_Name and Date first
    SELECT
      Ad_Name,
      Date,
      SUM(Spend) AS Spend,
      SUM(Revenue) AS Revenue,
      SUM(Purchases) AS Purchases
    FROM all_data
    GROUP BY Ad_Name, Date
  )
),
-- Get final lifetime totals
final_lifetime AS (
  SELECT
    Ad_Name,
    MAX(lifetime_spend) AS total_lifetime_spend,
    MAX(lifetime_revenue) AS total_lifetime_revenue,
    MAX(lifetime_conversions) AS total_lifetime_conversions,
    SAFE_DIVIDE(MAX(lifetime_revenue), MAX(lifetime_spend)) AS lifetime_roas
  FROM lifetime_metrics
  GROUP BY Ad_Name
),
-- Reported winners with a numeric level for ranking
reported AS (
  SELECT
    Ad_Name,
    CASE
      WHEN lifetime_spend >= 1000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6
           AND lifetime_spend < 5000 THEN 'Level 1 Winner'
      WHEN lifetime_spend >= 5000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6
           AND lifetime_spend < 20000 THEN 'Level 2 Winner'
      WHEN lifetime_spend >= 20000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6 THEN 'Level 3 Winner'
      ELSE NULL
    END AS winner_type,
    CASE
      WHEN lifetime_spend >= 20000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6 THEN 3
      WHEN lifetime_spend >= 5000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6 THEN 2
      WHEN lifetime_spend >= 1000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6 THEN 1
      ELSE NULL
    END AS level_num,
    dt AS achieved_date,
    lifetime_spend AS spend_at_level,
    lifetime_revenue AS revenue_at_level,
    lifetime_conversions AS conversions_at_level,
    SAFE_DIVIDE(lifetime_revenue, lifetime_spend) AS roas_at_level
  FROM lifetime_metrics
  WHERE lifetime_spend >= 1000 
    AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY Ad_Name,
    CASE
      WHEN lifetime_spend >= 1000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6
           AND lifetime_spend < 5000 THEN 'Level 1 Winner'
      WHEN lifetime_spend >= 5000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6
           AND lifetime_spend < 20000 THEN 'Level 2 Winner'
      WHEN lifetime_spend >= 20000 AND SAFE_DIVIDE(lifetime_revenue, lifetime_spend) >= 1.6 THEN 'Level 3 Winner'
    END
    ORDER BY dt
  ) = 1
),
-- Get the highest level achieved per Ad_Name
highest_level AS (
  SELECT
    Ad_Name,
    winner_type,
    level_num,
    achieved_date,
    spend_at_level,
    revenue_at_level,
    conversions_at_level,
    roas_at_level
  FROM reported
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Ad_Name ORDER BY level_num DESC, achieved_date DESC) = 1
),
-- Monthly max level per Ad_Name
monthly_max AS (
  SELECT
    Ad_Name,
    DATE_TRUNC(achieved_date, MONTH) AS month_start,
    MAX(level_num) AS month_max_level
  FROM reported
  GROUP BY Ad_Name, DATE_TRUNC(achieved_date, MONTH)
),
-- Get monthly performance for top performer tracking
monthly_performance AS (
  SELECT
    Ad_Name,
    FORMAT_DATE('%Y-%m', Date) AS month_year,
    SUM(Spend) AS monthly_spend,
    SUM(Revenue) AS monthly_revenue
  FROM all_data
  GROUP BY Ad_Name, FORMAT_DATE('%Y-%m', Date)
),
-- Rank ads within each month for top performer
monthly_rankings AS (
  SELECT
    Ad_Name,
    month_year,
    ROW_NUMBER() OVER (PARTITION BY month_year ORDER BY monthly_revenue DESC) AS revenue_rank
  FROM monthly_performance
  WHERE monthly_spend >= 100
),
-- Aggregate monthly top performer flags
monthly_top_performer_flags AS (
  SELECT
    Ad_Name,
    MAX(CASE WHEN revenue_rank = 1 THEN 1 ELSE 0 END) AS was_ever_monthly_top_performer,
    STRING_AGG(CASE WHEN revenue_rank = 1 THEN month_year END, ', ' ORDER BY month_year) AS months_as_top_performer
  FROM monthly_rankings
  GROUP BY Ad_Name
)
-- Final SELECT
SELECT
  b.Ad_Name,
  b.Ad_Id,
  b.campaign_id,
  b.campaign_name,
  b.adset_id,
  b.adset_name,
  b.ad_creative_link,
  
  -- 🆕 FIXED THUMBNAIL LOGIC WITH COALESCE
  COALESCE(
    b.main_thumbnail,
    MAX(cl.creative_thumbnail_url) OVER (PARTITION BY b.Ad_Name)
  ) AS Creative_Thumbnail_URL_Fixed,
  
  COALESCE(
    b.main_facebook_url,
    MAX(cl.creative_facebook_url) OVER (PARTITION BY b.Ad_Name)
  ) AS Creative_Facebook_URL_Fixed,
  
  b.creative_type,
  b.copywriter,
  b.CreativeStrategist,
  b.actor,
  b.editor,
  b.week_tag,
  b.awareness,
  b.angle,
  b.hook,
  b.product,
  b.Brief_Number,
  fl.total_lifetime_spend   AS lifetime_spend,
fl.total_lifetime_revenue AS lifetime_revenue,
  b.iteration_netnew,
  b.latest_date AS date,
  
  fl.total_lifetime_spend,
  fl.total_lifetime_revenue,
  fl.total_lifetime_conversions,
  fl.lifetime_roas,
  
  hl.winner_type AS highest_winner_type,
  hl.level_num AS highest_level_num,
  hl.achieved_date AS highest_level_achieved_date,
  hl.spend_at_level,
  hl.revenue_at_level,
  hl.conversions_at_level,
  hl.roas_at_level,
  
  -- Reported metrics (same as spend/revenue at level for first achievement)
  hl.spend_at_level AS reported_spend,
  hl.revenue_at_level AS reported_revenue,
  hl.roas_at_level AS reported_roas,
  
  -- Monthly-highest flag
  CASE
    WHEN hl.achieved_date IS NOT NULL
         AND hl.level_num = mm.month_max_level
         AND DATE_TRUNC(hl.achieved_date, MONTH) = mm.month_start
      THEN 1 
    ELSE 0
  END AS is_monthly_highest_flag,
  
  -- Bonus
  CASE
    WHEN hl.level_num = 1 THEN 25
    WHEN hl.level_num = 2 THEN 50
    WHEN hl.level_num = 3 THEN 150
    ELSE 0
  END AS bonus_amount,
  
  COALESCE(mtpf.was_ever_monthly_top_performer, 0) AS was_monthly_top_performer,
  mtpf.months_as_top_performer
FROM base_with_link b
LEFT JOIN final_lifetime fl
  ON fl.Ad_Name = b.Ad_Name
LEFT JOIN highest_level hl
  ON hl.Ad_Name = b.Ad_Name
LEFT JOIN monthly_max mm
  ON mm.Ad_Name = hl.Ad_Name
  AND DATE_TRUNC(hl.achieved_date, MONTH) = mm.month_start
LEFT JOIN monthly_top_performer_flags mtpf
  ON mtpf.Ad_Name = b.Ad_Name
-- 🔗 LEFT JOIN for creative links
LEFT JOIN creative_links cl
  ON cl.Ad_Name = b.Ad_Name
ORDER BY b.Ad_Name;
