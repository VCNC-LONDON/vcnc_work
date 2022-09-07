SELECT
  review.*,
  tags.tags
FROM
  tada.trip_review_by_driver review
LEFT JOIN (
  SELECT
    trip_review_id,
    array_agg(tag) as tags
  FROM
    tada.trip_review_by_driver_tag
  GROUP BY
    trip_review_id
) tags 
on review.id = tags.trip_review_id
