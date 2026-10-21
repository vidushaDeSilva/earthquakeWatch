-- Read-only serving surface. Consumers receive SELECT on these two views only.
-- The pointer resolves the current immutable publication on every query.
CREATE VIEW IF NOT EXISTS earthquake_dev.gold.active_watchlist AS
SELECT w.*
FROM earthquake_dev.gold.watchlist_snapshots w
JOIN earthquake_dev.ops.serving_pointer p
  ON p.product_key = 'usgs-watchlist'
 AND p.active_publication_id = w.publication_id
JOIN earthquake_dev.ops.publications pub
  ON pub.publication_id = p.active_publication_id
 AND pub.publication_status = 'ACTIVE'
 AND pub.validation_status = 'PASS';

CREATE VIEW IF NOT EXISTS earthquake_dev.gold.active_daily_metrics AS
SELECT d.*
FROM earthquake_dev.gold.daily_metrics d
JOIN earthquake_dev.ops.serving_pointer p
  ON p.product_key = 'usgs-watchlist'
 AND p.active_publication_id = d.publication_id
JOIN earthquake_dev.ops.publications pub
  ON pub.publication_id = p.active_publication_id
 AND pub.publication_status = 'ACTIVE'
 AND pub.validation_status = 'PASS';
