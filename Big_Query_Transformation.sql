SELECT
 -- 🆔 Identifiers
f.ad_id AS `Ad_Id`,
f.ad_name AS `Ad_Name`,
f.campaign_id AS `Campaign_Id`,
f.campaign AS `Campaign_Name`,
f.adset_id AS `Adset_Id`,
f.adset_name AS `Adset_Name`,
SAFE_CAST(f.ad_created_time AS TIMESTAMP) AS `Adset_Created_Time`,


-- 🏷 Campaign taxonomy
SPLIT(
  TRIM(f.campaign),
  CASE WHEN STRPOS(f.campaign, '>') > 0 THEN '>' ELSE '-' END
)[SAFE_OFFSET(1)] AS Campaign_Type,


SPLIT(
  TRIM(f.campaign),
  CASE WHEN STRPOS(f.campaign, '>') > 0 THEN '>' ELSE '-' END
)[SAFE_OFFSET(2)] AS Campaign_Format,


-- 🏷 Ad name taxonomy (dual logic with OLD naming convention)
CASE WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, ' - ')[SAFE_OFFSET(0)]
  ELSE SPLIT(f.ad_name, '_')[SAFE_OFFSET(0)]
END AS Brief_Number,


CASE WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, ' - ')[SAFE_OFFSET(1)] END AS Hook,


COALESCE(
  UPPER(REGEXP_EXTRACT(f.ad_name, r'(?i)(?:^|[\s\-])(IT|NN)(?:[\s\-]|$)')),
  'NN'
) AS Iteration_X_NetNew,


CASE WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, ' - ')[SAFE_OFFSET(3)] END AS Parent_Brief_Id,


CASE 
  WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
    THEN SPLIT(f.ad_name, ' - ')[SAFE_OFFSET(4)]
  ELSE SPLIT(f.ad_name, '_')[SAFE_OFFSET(5)]
END AS Angle,


CASE 
  WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
    THEN UPPER(REGEXP_EXTRACT(f.ad_name, r'(?i)(Mini A|B|V|MF|L)'))
  ELSE UPPER(REGEXP_EXTRACT(f.ad_name, r'(?i)(F|G|D|E|X)'))
END AS CreativeType,


CASE WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, ' - ')[SAFE_OFFSET(6)]
  ELSE SPLIT(f.ad_name, '_')[SAFE_OFFSET(6)]
END AS Copywriter,


CASE WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN "NA" END AS NA_Col,


-- 🎬 Editor - Using regex match (case-insensitive) for both naming conventions
UPPER(REGEXP_EXTRACT(f.ad_name, r'(?i)(B|A|L|O|H)')) AS Editor,


CASE WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, ' - ')[SAFE_OFFSET(9)] END AS Week_Tag,


CASE WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, ' - ')[SAFE_OFFSET(10)] END AS Awareness,


-- 🆕 Ad Launch Date from Week_Tag
CASE
  WHEN REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}') THEN
    DATE_ADD(
      DATE_TRUNC(CURRENT_DATE(), YEAR),
      INTERVAL (SAFE_CAST(REGEXP_EXTRACT(f.ad_name, r'WK\s?(\d+)') AS INT64) - 1) WEEK
    )
END AS Ad_Launch_Date,


-- 📦 Product - Using regex match (case-insensitive) for both naming conventions
UPPER(REGEXP_EXTRACT(f.ad_name, r'(?i)(IO|THS)')) AS Product,


CASE WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(3)] END AS SubAngle,


CASE
  WHEN REGEXP_CONTAINS(f.ad_name, r'(?i)lC') THEN 'Lucia'
  WHEN REGEXP_CONTAINS(f.ad_name, r'(?i)aDDo') THEN 'Jon'
  WHEN REGEXP_CONTAINS(f.ad_name, r'(?i)cPPn') THEN 'Cosmin'
  WHEN REGEXP_CONTAINS(f.ad_name, r'(?i)jKK') THEN 'Jon'
  WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
    THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(5)]
END AS CreativeStrategist,


CASE WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(8)] END AS Actor,


CASE WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(9)] END AS Script,


CASE WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(10)] END AS Mechanism,


CASE WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(11)] END AS ScrollStopper_3sec_vid_ID,


CASE WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(12)] END AS Hook_text_3secs_ID,


CASE WHEN NOT REGEXP_CONTAINS(f.ad_name, r'^B[0-9]{3}')
  THEN SPLIT(f.ad_name, '_')[SAFE_OFFSET(13)] END AS RootCause,


-- 🔗 Creative Links - Fixed with proper deduplication using MAX
COALESCE(
  NULLIF(TRIM(f.Creative_Thumbnail_URL), ''),
  MAX(lt.`Creative_thumbnail_URL`) OVER (PARTITION BY f.ad_name, f.date, f.ad_id)
) AS `Creative_Thumbnail_URL_Fixed`,

COALESCE(
  NULLIF(TRIM(f.link_url), ''),
  MAX(lt.`Creative_Facebook_URL`) OVER (PARTITION BY f.ad_name, f.date, f.ad_id)
) AS `Creative_Facebook_URL_Fixed`,


-- 📊 Core metrics
f.action_values_omni_purchase AS Revenue,
f.actions_add_to_cart AS Add_to_Cart,
f.actions_initiate_checkout AS Initiate_Checkout,
f.actions_offsite_conversion_fb_pixel_purchase AS Purchases,
f.`3-second video plays` AS `3SecondVideoPlays`,
f.video_avg_time_watched_actions_video_view AS `Average_Play_Time`,
f.video_p100_watched_actions_video_view AS `VideoPlaysat100`,
f.video_play_actions_video_view AS `Video_Plays`,


-- 📈 Engagement & spend
f.clicks AS Clicks,
f.impressions AS Impressions,
f.link_clicks AS Link_Clicks,
f.link_url AS `Ad_Creative_Link`,
f.spend AS Spend,


-- 🕒 Creative creation time
SAFE_CAST(f.ad_created_time AS TIMESTAMP) AS `Ad_Created_Time`,
f.date AS Date,


-- 📅 Week starting Sunday
DATE_TRUNC(f.Date, WEEK(SUNDAY)) AS `Week_First_Day`,


-- ⏳ Days since creative launched
DATE_DIFF(
  CURRENT_DATE(),
  CAST(SAFE_CAST(f.ad_created_time AS TIMESTAMP) AS DATE),
  DAY
) AS `Days_Till_Creative_Launched`,


-- ✅ Correct earliest adset creation date
CAST(
  MIN(SAFE_CAST(f.ad_created_time AS TIMESTAMP))
  OVER (PARTITION BY f.ad_name) AS DATE
) AS `Adset_First_Created_Time`


FROM `mythical-willow-431913-s4.Test1.Facebook_import` f


-- 🔗 LEFT JOIN with proper ROW_NUMBER to prevent duplicates
LEFT JOIN (
SELECT
  `Ad_Name`,
  `Creative_thumbnail_URL`,
  `Creative_Facebook_URL`
FROM (
  SELECT
    `Ad_Name`,
    `Creative_thumbnail_URL`,
    `Creative_Facebook_URL`,
    ROW_NUMBER() OVER (PARTITION BY `Ad_Name` ORDER BY `Ad_Name`) AS rn
  FROM `mythical-willow-431913-s4.Test1.Apache_Thumbnail_ETL`
)
WHERE rn = 1
) lt
ON f.ad_name = lt.`Ad_Name`


-- 🚫 Include NULL dates as well
WHERE CAST(SAFE_CAST(f.ad_created_time AS TIMESTAMP) AS DATE) <= CURRENT_DATE()
  OR f.ad_created_time IS NULL;
