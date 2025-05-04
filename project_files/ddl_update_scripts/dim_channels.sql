-- ⚠️ CAUTION:
-- This script drops and recreates the 'dimchannels' table in the silver layer.
-- Ensure a **complete backup** of the data is taken before executing.
-- This should only be run if the existing table and its data can be safely removed.
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
--   Updates the 'dimChannels' dimension table in the Silver layer by 
--   inserting new records from the 'bronze.subscribers_views_videos' source table.
--   Only inserts records where 'playlistID' is NOT NULL and does not already
--   exist in 'dimChannels', ensuring no duplicate entries based on 'playlistID'.
--
-- BEHAVIOR:
--   - Performs an incremental insert (no updates or deletes).
--   - Uses 'playlistID' as the unique reference for new entries.
--   - Logs a success message upon completion using RAISE NOTICE.
--
-- USAGE CAUTION:
--   Ensure the procedure is executed in a controlled environment to avoid 
--   inserting incomplete or invalid data from the Bronze layer.
--
-- LAST UPDATED: [Add Date]
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