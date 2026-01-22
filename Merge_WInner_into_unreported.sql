MERGE `mythical-willow-431913-s4.Test1.unreported_winners` AS T
USING (
  /* 1) Base source */
  WITH all_data AS (
    SELECT *
    FROM `mythical-willow-431913-s4.Test1.Facebook_Final_New`
  ),

  /* 2) Facebook creative URLs — FINAL_NEW only, Facebook-only */
  facebook_creative_urls AS (
    SELECT
      Ad_Name,
      MAX(Creative_Facebook_URL_Fixed) AS creative_facebook_url
    FROM all_data
    WHERE Creative_Facebook_URL_Fixed IS NOT NULL
      AND (
        STARTS_WITH(Creative_Facebook_URL_Fixed, 'https://facebook.com')
        OR STARTS_WITH(Creative_Facebook_URL_Fixed, 'https://www.facebook.com')
      )
    GROUP BY Ad_Name
  ),

  /* 3) Base metadata per Ad_Name */
  base AS (
    SELECT
      Ad_Name,
      ANY_VALUE(Ad_Id) AS Ad_Id,
      ANY_VALUE(Campaign_Id) AS campaign_id,
      ANY_VALUE(Campaign_Name) AS campaign_name,
      ANY_VALUE(Adset_Id) AS adset_id,
      ANY_VALUE(Adset_Name) AS adset_name,
      ANY_VALUE(CreativeType) AS creative_type,
      ANY_VALUE(Copywriter) AS copywriter,
      ANY_VALUE(Actor) AS actor,
      ARRAY_AGG(Editor IGNORE NULLS ORDER BY Date ASC LIMIT 1)[OFFSET(0)] AS editor,
      ANY_VALUE(Week_Tag) AS week_tag,
      ANY_VALUE(Awareness) AS awareness,
      ANY_VALUE(Angle) AS angle,
      ANY_VALUE(Hook) AS hook,
      ANY_VALUE(Product) AS product,
      ANY_VALUE(Brief_Number) AS Brief_Number,
      ARRAY_AGG(Iteration_X_NetNew ORDER BY Date ASC LIMIT 1)[OFFSET(0)] AS iteration_netnew,
      MAX(Date) AS latest_date
    FROM all_data
    GROUP BY Ad_Name
  ),

  /* 4) Lifetime metrics */
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

  /* 5) Winner detection */
  reported AS (
    SELECT
      Ad_Name,
      CASE
        WHEN lifetime_spend >= 20000 THEN 'Level 3 Winner'
        WHEN lifetime_spend >= 5000 THEN 'Level 2 Winner'
        WHEN lifetime_spend >= 1000 THEN 'Level 1 Winner'
      END AS winner_type,
      CASE
        WHEN lifetime_spend >= 20000 THEN 3
        WHEN lifetime_spend >= 5000 THEN 2
        WHEN lifetime_spend >= 1000 THEN 1
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
          WHEN lifetime_spend >= 20000 THEN 3
          WHEN lifetime_spend >= 5000 THEN 2
          WHEN lifetime_spend >= 1000 THEN 1
        END
      ORDER BY dt
    ) = 1
  ),

  /* 6) Highest level per Ad */
  highest_level AS (
    SELECT *
    FROM reported
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY Ad_Name
      ORDER BY level_num DESC, achieved_date DESC
    ) = 1
  ),

  /* 7) Historical max level already reported */
  reported_overall AS (
    SELECT
      ad_name,
      MAX(
        CASE winner_type
          WHEN 'Level 1' THEN 1
          WHEN 'Level 2' THEN 2
          WHEN 'Level 3' THEN 3
        END
      ) AS max_level_overall
    FROM `mythical-willow-431913-s4.Test1.reported_winners`
    GROUP BY ad_name
  ),

  /* 8) Final candidate rows */
  final_candidates AS (
    SELECT
      b.Ad_Id AS ad_id,
      b.Ad_Name AS ad_name,
      hl.spend_at_level AS spend,
      hl.revenue_at_level AS revenue,
      b.campaign_name,
      hl.roas_at_level AS roas,
      b.editor,
      b.creative_type AS brief_type,
      'Default' AS segment,
      fcu.creative_facebook_url,
      DATE(hl.achieved_date) AS achieved_date,
      hl.winner_type,
      CASE
        WHEN hl.level_num = 1 THEN 25
        WHEN hl.level_num = 2 THEN 50
        WHEN hl.level_num = 3 THEN 150
      END AS bonus,
      FALSE AS is_same_month_progression,
      FALSE AS is_cross_month_progression,
      CAST(NULL AS STRING) AS previous_level,
      CAST(NULL AS DATE) AS previous_achievement_date,
      CAST(NULL AS INT64) AS months_since_last_level,
      'New Winner' AS progression_path,
      hl.level_num,
      COALESCE(ro.max_level_overall, 0) AS hist_max_level
    FROM base b
    JOIN highest_level hl
      ON hl.Ad_Name = b.Ad_Name
    LEFT JOIN facebook_creative_urls fcu
      ON fcu.Ad_Name = b.Ad_Name
    LEFT JOIN reported_overall ro
      ON ro.ad_name = b.Ad_Name
  )

  /* 9) Filter only new progressions */
  SELECT
    ad_id,
    ad_name,
    spend,
    revenue,
    campaign_name,
    roas,
    editor,
    brief_type,
    segment,
    creative_facebook_url,
    achieved_date,
    winner_type,
    bonus,
    is_same_month_progression,
    is_cross_month_progression,
    previous_level,
    previous_achievement_date,
    months_since_last_level,
    progression_path
  FROM final_candidates
  WHERE level_num > hist_max_level
    AND NOT EXISTS (
      SELECT 1
      FROM `mythical-willow-431913-s4.Test1.reported_winners` r
      WHERE r.ad_name = final_candidates.ad_name
        AND r.winner_type = final_candidates.winner_type
    )
) AS S
ON (
  T.ad_id = S.ad_id
  AND T.ad_name = S.ad_name
  AND T.achieved_date = S.achieved_date
  AND T.winner_type = S.winner_type
)
WHEN NOT MATCHED THEN
INSERT (
  ad_id,
  ad_name,
  spend,
  revenue,
  campaign_name,
  roas,
  editor,
  brief_type,
  segment,
  creative_facebook_url,
  achieved_date,
  winner_type,
  bonus,
  is_same_month_progression,
  is_cross_month_progression,
  previous_level,
  previous_achievement_date,
  months_since_last_level,
  progression_path
)
VALUES (
  S.ad_id,
  S.ad_name,
  S.spend,
  S.revenue,
  S.campaign_name,
  S.roas,
  S.editor,
  S.brief_type,
  S.segment,
  S.creative_facebook_url,
  S.achieved_date,
  S.winner_type,
  S.bonus,
  S.is_same_month_progression,
  S.is_cross_month_progression,
  S.previous_level,
  S.previous_achievement_date,
  S.months_since_last_level,
  S.progression_path
);
