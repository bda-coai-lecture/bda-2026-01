{% docs weekly_active_user_retention_heatmap %}

## GitHub Core Weekly Active User Retention Heatmap

이 모델은 "첫 활동 주차 cohort retention"이 아니라, 각 주의 전체 WAU를 기준 집단으로 잡는 active user retention heatmap이다.

한 행은 `week_start` 주에 GitHub Archive에서 1회 이상 활동한 모든 유저 집합을 나타낸다. `active_users`는 그 주의 WAU이며, `metrics_weekly.weekly_active_users`와 같은 값이어야 한다. `w1`부터 `w12`는 이 기준 집단 중 이후 N주차에도 다시 활동한 유저 비율이다.

계산식:

```text
wN = count(distinct user_id where user_id active in week_start and week_start + N weeks)
     / count(distinct user_id where user_id active in week_start)
```

예시:

```text
week_start = 2026-04-06
active_users = 100,000
w1 = 0.42
w2 = 0.31
```

해석:

- 2026-04-06 주에 활동한 유저는 100,000명이다.
- `w1 = 0.42`는 그 100,000명 중 42,000명이 2026-04-13 주에도 활동했다는 뜻이다.
- `w2 = 0.31`은 같은 기준 집단 100,000명 중 31,000명이 2026-04-20 주에도 활동했다는 뜻이다.

주의:

- 기준 집단은 매주 새로 잡힌다. 따라서 같은 유저가 여러 주의 기준 집단에 반복 포함될 수 있다.
- 신규 유저만 보는 first-seen cohort가 아니다.
- 여기서 유저는 GitHub Archive `actor.id` 기준이다. 예를 들어 A 유저가 B organization 아래에서 이벤트를 발생시키면 기준 집단에는 B가 아니라 A가 들어간다. 현재 모델은 GitHub Archive `org` context를 저장하지 않는다.
- 최신 주는 아직 미래 주차가 없어서 `w1` 이후 값이 null일 수 있다. null은 아직 관측할 수 없는 미래 기간이라는 뜻이며, 0% retention으로 해석하면 안 된다.
- Metabase 색상 범위는 현재 분포에 맞춰 대략 15%~55%로 잡아 둔다. 분포가 크게 바뀌면 dashboard 설정만 조정하면 된다.

{% enddocs %}

{% docs monthly_active_user_retention_heatmap %}

## GitHub Core Monthly Active User Retention Heatmap

이 모델은 각 월의 전체 MAU를 기준 집단으로 잡는 monthly active user retention heatmap이다.

한 행은 `month_start` 월에 GitHub Archive에서 1회 이상 활동한 모든 유저 집합을 나타낸다. `active_users`는 그 월의 MAU이며, `m1`부터 `m12`는 이 기준 집단 중 이후 N개월차에도 다시 활동한 유저 비율이다.

계산식:

```text
mN = count(distinct user_id where user_id active in month_start and month_start + N months)
     / count(distinct user_id where user_id active in month_start)
```

예시:

```text
month_start = 2026-03-01
active_users = 1,200,000
m1 = 0.38
m2 = 0.29
```

해석:

- 2026-03-01 월에 활동한 유저는 1,200,000명이다.
- `m1 = 0.38`은 그 1,200,000명 중 456,000명이 2026-04-01 월에도 활동했다는 뜻이다.
- `m2 = 0.29`는 같은 기준 집단 1,200,000명 중 348,000명이 2026-05-01 월에도 활동했다는 뜻이다.

주의:

- 기준 집단은 매월 새로 잡힌다. 같은 유저가 여러 월의 기준 집단에 반복 포함될 수 있다.
- 신규 유저만 보는 first-seen cohort가 아니다.
- 여기서 유저는 GitHub Archive `actor.id` 기준이다. 예를 들어 A 유저가 B organization 아래에서 이벤트를 발생시키면 기준 집단에는 B가 아니라 A가 들어간다. 현재 모델은 GitHub Archive `org` context를 저장하지 않는다.
- 아직 관측되지 않은 미래 월은 null로 남긴다. 0은 실제 0% retention일 때만 써야 하므로, 이 모델에서는 미래 기간을 0으로 채우지 않는다.
- Metabase 색상 범위는 현재 분포에 맞춰 대략 20%~50%로 잡아 둔다. 분포가 크게 바뀌면 dashboard 설정만 조정하면 된다.

{% enddocs %}
