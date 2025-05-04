-- ============================================
-- TABLE: silver.fct_video_statistics
-- DESCRIPTION:
--   Stores aggregated statistics for each video at the time of ingestion.
--   Metrics include:
--     - View count
--     - Like count
--     - Favourite count
--     - Comment count
--   Joined using video IDs from the dimension table to ensure consistency.
--
-- NOTES:
--   - 'video_id' and 'channel_id' are references to the dimension table 'silver.dimvideos'.
--   - 'ingestion_time' captures when the record was inserted for auditing and tracking.
-- ============================================
CREATE TABLE silver.fct_video_statistics (
    video_id int8 NULL,
    channel_id int4 NULL,
    ingestion_time timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    view_count int4 NULL,
    like_count int4 NULL,
    favourite_count int4 NULL,
    comment_count int4 NULL
);
-- ============================================
-- PROCEDURE: silver.sp_update_fct_video_statistics
-- DESCRIPTION:
--   Inserts video-level statistics from the 'bronze.video_statistics' source
--   into the 'fct_video_statistics' fact table in the Silver layer.
--   Joins data to video and channel IDs from 'silver.dimvideos'.
--
-- LOGIC:
--   - Performs casting of raw counts to INT for uniformity.
--   - Ensures dimensional alignment by joining on 'videoID'.
--   - Automatically captures the ingestion timestamp.
--
-- BEHAVIOR:
--   - Additive insert only; does not perform updates or deletions.
--   - Can be scheduled for regular execution to capture periodic stats.
--
-- USAGE CAUTION:
--   Ensure 'bronze.video_statistics' contains the latest and clean data.
--   Duplicate video entries in 'dimvideos' may cause skewed statistics.
--
-- LAST UPDATED: [Add Date Here]
-- ============================================
CREATE OR REPLACE PROCEDURE silver.sp_update_fct_video_statistics() LANGUAGE plpgsql AS $procedure$ BEGIN
INSERT INTO silver.fct_video_statistics (
        video_id,
        channel_id,
        view_count,
        like_count,
        favourite_count,
        comment_count
    )
SELECT d.video_id,
    d.channel_id,
    CAST(vs."viewCount" AS INT) AS view_count,
    CAST(vs."likeCount" AS INT) AS like_count,
    CAST(vs."favoriteCount" AS INT) AS favourite_count,
    CAST(vs."commentCount" AS INT) AS comment_count
FROM bronze.video_statistics vs
    LEFT JOIN silver.dimvideos d ON d.videoID = vs.video_id;
END;
$procedure$;