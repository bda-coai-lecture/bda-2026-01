select *
from {{ ref('fct_push_repo_episode') }}
where last_push_date < episode_start_date
   or churn_date != date_add(last_push_date, interval 28 day)
   or episode_duration_days != date_diff(last_push_date, episode_start_date, day) + 1
   or entry_actor_count < 1
   or lifetime_actor_count < entry_actor_count
   or total_push_events < lifetime_actor_count
   or active_weeks < 1
   or active_weeks > entropy_calendar_weeks
   or entropy_calendar_weeks < 1
   or entropy_nats < -1e-12
   or normalized_entropy < -1e-12
   or normalized_entropy > 1 + 1e-12
   or (entropy_calendar_weeks = 1 and abs(normalized_entropy) > 1e-12)
   or (
     transitioned_to_collaboration
     and not (entry_actor_count = 1 and lifetime_actor_count >= 2)
   )
   or (is_collaborative_at_entry and not is_ever_collaborative)
   or (is_collaborative_at_entry and transitioned_to_collaboration)
   or (
     previous_episode_last_push_date is not null
     and days_since_previous_episode_push <= 28
   )
   or (
     next_episode_start_date is not null
     and date_diff(next_episode_start_date, last_push_date, day) <= 28
   )
   or is_churn_observable = is_provisional
