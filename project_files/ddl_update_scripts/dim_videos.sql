-- ============================================
-- ⚠️ CAUTION: DROP AND RECREATE TABLE
-- This script drops and recreates the 'dimchannels' table in the Silver layer.
-- Ensure a full backup of the existing data is taken before execution.
-- Execute this script only when it is safe to lose existing data.
-- ============================================
DROP TABLE IF EXISTS silver.dimchannels;
CREATE TABLE silver.dimchannels (
    channel_id serial4 NOT NULL,
    channel_name varchar(50) NULL,
    channel_description text NULL,
    channel_country varchar(5) NULL,
    channel_playlist_id text NULL,
    CONSTRAINT dimchannels_pkey PRIMARY KEY (channel_id)
);
-- ============================================
-- PROCEDURE: silver.sp_update_dim_channels
-- DESCRIPTION:
--   Populates the 'dimChannels' dimension table in the Silver layer
--   by inserting new channel records from 'bronze.subscribers_views_videos'.
--
-- LOGIC:
--   - Inserts only records with non-null 'playlistID'.
--   - Skips records already existing in 'dimChannels', based on 'playlistID'.
--   - Avoids duplicate inserts and ensures incremental population.
--   - Uses SELECT DISTINCT to improve data quality (if needed).
--
-- BEHAVIOR:
--   - No updates or deletions performed; purely additive logic.
--   - Raises a NOTICE upon successful completion.
--
-- USAGE CAUTION:
--   Ensure the source data from the Bronze layer is clean and reliable
--   before running this procedure to avoid dirty inserts.
--
-- LAST UPDATED: [Add Date Here]
-- ============================================
CREATE OR REPLACE PROCEDURE silver.sp_update_dim_channels() LANGUAGE plpgsql AS $procedure$ BEGIN
INSERT INTO silver.dimChannels (
        channel_name,
        channel_description,
        channel_country,
        channel_playlist_id
    )
SELECT title AS channel_name,
    description AS channel_description,
    country AS channel_country,
    "playlistID" AS channel_playlist_id
FROM bronze.subscribers_views_videos
WHERE "playlistID" IS NOT NULL
    AND "playlistID" NOT IN (
        SELECT channel_playlist_id
        FROM silver.dimChannels
    );
RAISE NOTICE '✅ dimChannels table updated successfully.';
END;
$procedure$;