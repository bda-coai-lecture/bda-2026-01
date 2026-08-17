-- Question: non-Push 급감이 상류 raw shard 자체에 있는가 (mart를 거치지 않고 직접 확인)
-- Grain: activity_date (UTC shard)
-- Time basis / timezone: _TABLE_SUFFIX (UTC 일자 shard)
-- Validation range: 2026-07-02 / 2026-07-21 (raw vs fact 정합 — 별도 파일)
-- Analysis range: 2026-06-15 ~ 2026-07-25 (_TABLE_SUFFIX 명시 범위. 와일드카드 무제한 스캔 금지)
-- Segments: PushEvent vs non-Push
-- Exclusions: actor.id / repo.id / type null 제외 (fact 적재 조건과 동일). automation_actor: 포함
-- Metric definition: raw count(*) per day
-- Expected output grain: activity_date 당 1행

select
  parse_date('%Y%m%d', concat('20', _table_suffix)) as activity_date,
  countif(type = 'PushEvent') as raw_push,
  countif(type <> 'PushEvent') as raw_non_push,
  count(distinct if(type <> 'PushEvent', type, null)) as raw_non_push_types
from `githubarchive.day.20*`
where _table_suffix between '260615' and '260725'
  and actor.id is not null
  and repo.id is not null
  and type is not null
group by activity_date
order by activity_date
