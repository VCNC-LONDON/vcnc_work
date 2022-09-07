select
  review.*,
  tags.tags
from
  tada.ride_review_by_user review
  left join (
    select
      ride_review_id,
      array_agg(tag) as tags
    from
      tada.ride_review_by_user_tag
    group by
      ride_review_id
  ) tags on review.id = tags.ride_review_id
