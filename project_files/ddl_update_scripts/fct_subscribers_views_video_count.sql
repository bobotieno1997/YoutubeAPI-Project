-- ============================================
-- TABLE: silver.fct_subscribers_views_video_count
-- DESCRIPTION:
--   Fact table in the Silver layer to store metrics for each channel, including:
--     - Subscriber count
--     - View count
--     - Video count
--   The data is joined using 'channel_playlist_id' from the dimension table.
--   Each record also captures the timestamp of ingestion.
--
-- NOTES:
--   - Designed for snapshot-style ingestion of channel metrics.
--   - 'channel_id' is a foreign key reference to 'silver.dimchannels'.
--   - 'ingestion_date' defaults to current timestamp.
-- ============================================
CREATE TABLE silver.fct_subscribers_views_video_count (
    channel_id int2 NULL,
    ingestion_date timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    subscriber_count int8 NULL,
    view_count int8 NULL,
    video_count int8 NULL
);
-- ============================================
-- PROCEDURE: silver.sp_update_fct_subscribers_views_video_count
-- DESCRIPTION:
--   Populates the 'fct_subscribers_views_video_count' fact table
--   by extracting and transforming metrics from 'bronze.subscribers_views_videos'.
--   Joins each record to its corresponding channel in 'silver.dimchannels'.
--
-- LOGIC:
--   - Joins on 'channel_playlist_id' from the dimension table.
--   - Casts subscriber, view, and video counts to BIGINT for storage.
--   - Appends data with a timestamp (via 'ingestion_date' default).
--
-- BEHAVIOR:
--   - Purely additive insert process (no updates or deletes).
--   - Suitable for time-series tracking or snapshot-based reporting.
--
-- USAGE CAUTION:
--   Ensure data quality in 'bronze.subscribers_views_videos' before execution.
--
-- LAST UPDATED: [Add Date Here]
-- ============================================
CREATE OR REPLACE PROCEDURE silver.sp_update_fct_subscribers_views_video_count() LANGUAGE plpgsql AS $procedure$ BEGIN
INSERT INTO silver.fct_subscribers_views_video_count (
        channel_id,
        subscriber_count,
        view_count,
        video_count
    )
SELECT d.channel_id,
    CAST(svv."subscriberCount" AS BIGINT),
    CAST(svv."viewCount" AS BIGINT),
    CAST(svv."videoCount" AS BIGINT)
FROM bronze.subscribers_views_videos svv
    LEFT JOIN silver.dimchannels d ON d.channel_playlist_id = svv."playlistID";
END;
$procedure$;