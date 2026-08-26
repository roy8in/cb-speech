"""Reference sentiment derivations for future Snowflake CORE models.

These transformations intentionally operate only on the local SQLite data.
They do not upload data anywhere. The logic is kept in Python so the future
Snowflake SQL implementation can be checked against the existing behavior.
"""

from datetime import datetime

import pandas as pd


class SentimentDeriver:
    """Build speech-level and daily sentiment data from SQLite."""

    def __init__(self, db=None):
        if db is None:
            from .models import SpeechDB

            db = SpeechDB()
        self.db = db

    def get_events_df(self):
        """Return speech-level sentiment rows used as a CORE reference."""
        conn = self.db._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                    s.id AS speech_id,
                    s.url,
                    substr(s.date, 1, 10) AS date,
                    s.bank_code,
                    m.name AS speaker,
                    s.title,
                    ar.stance_score,
                    ar.stance_reason,
                    ar.keywords,
                    ar.main_risk,
                    ar.analysis_status,
                    ar.analyzed_at,
                    s.fetched_at,
                    s.created_at
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                JOIN analysis_results ar ON s.id = ar.speech_id
                WHERE ar.analysis_status IN ('scored', 'no_signal')
                ORDER BY s.bank_code, date, s.id
                """
            ).fetchall()
            df = pd.DataFrame([dict(row) for row in rows])
        finally:
            conn.close()

        if df.empty:
            return df

        df["speech_id"] = pd.to_numeric(
            df["speech_id"], errors="coerce"
        ).astype("Int64")
        df["date"] = pd.to_datetime(
            df["date"], errors="coerce"
        ).dt.date.astype("string")

        for column in ("analyzed_at", "fetched_at", "created_at"):
            df[column] = pd.to_datetime(df[column], errors="coerce")

        speech_date = pd.to_datetime(df["date"], errors="coerce")
        df["collection_lag_days"] = (
            df["fetched_at"].dt.normalize() - speech_date
        ).dt.days.astype("Int64")
        df["analysis_lag_days"] = (
            df["analyzed_at"].dt.normalize() - speech_date
        ).dt.days.astype("Int64")
        df["updated_at"] = datetime.now().isoformat()

        columns = [
            "speech_id",
            "url",
            "date",
            "bank_code",
            "speaker",
            "title",
            "stance_score",
            "stance_reason",
            "keywords",
            "main_risk",
            "analysis_status",
            "analyzed_at",
            "fetched_at",
            "created_at",
            "collection_lag_days",
            "analysis_lag_days",
            "updated_at",
        ]
        return df[columns]

    def get_daily_df(self, half_life_days=14, fresh_days=45):
        """Return bank-date sentiment state used as a CORE reference."""
        events = self.get_events_df()
        if events.empty:
            return events

        scored = events[
            (events["analysis_status"] == "scored")
            & events["stance_score"].notna()
        ].copy()
        scored["date"] = pd.to_datetime(scored["date"], errors="coerce")

        speech_counts = (
            events.assign(
                date=pd.to_datetime(events["date"], errors="coerce")
            )
            .groupby(["bank_code", "date"], as_index=False)
            .agg(daily_total_speech_count=("speech_id", "count"))
        )

        daily_scores = (
            scored.groupby(["bank_code", "date"], as_index=False)
            .agg(
                daily_stance_score=("stance_score", "mean"),
                daily_scored_speech_count=("speech_id", "count"),
            )
        )

        banks = sorted(events["bank_code"].dropna().unique())
        end_date = pd.Timestamp.today().normalize()
        frames = []

        for bank in banks:
            bank_dates = pd.to_datetime(
                events.loc[events["bank_code"] == bank, "date"],
                errors="coerce",
            )
            start_date = bank_dates.min()
            if pd.isna(start_date):
                continue

            frames.append(
                pd.DataFrame(
                    {
                        "date": pd.date_range(
                            start_date.normalize(),
                            end_date,
                            freq="D",
                        ),
                        "bank_code": bank,
                    }
                )
            )

        if not frames:
            return pd.DataFrame()

        daily = pd.concat(frames, ignore_index=True)
        daily = daily.merge(
            daily_scores,
            how="left",
            on=["bank_code", "date"],
        )
        daily = daily.merge(
            speech_counts,
            how="left",
            on=["bank_code", "date"],
        )

        daily["daily_scored_speech_count"] = (
            daily["daily_scored_speech_count"].fillna(0).astype("Int64")
        )
        daily["daily_total_speech_count"] = (
            daily["daily_total_speech_count"].fillna(0).astype("Int64")
        )
        daily["has_scored_speech"] = (
            daily["daily_scored_speech_count"] > 0
        ).astype("Int64")

        daily = daily.sort_values(["bank_code", "date"])
        daily["stance_level_locf"] = daily.groupby("bank_code")[
            "daily_stance_score"
        ].ffill()
        daily["last_scored_speech_date"] = daily["date"].where(
            daily["has_scored_speech"].eq(1)
        )
        daily["last_scored_speech_date"] = daily.groupby("bank_code")[
            "last_scored_speech_date"
        ].ffill()
        daily["days_since_last_scored_speech"] = (
            daily["date"] - daily["last_scored_speech_date"]
        ).dt.days.astype("Int64")

        daily["freshness_weight_hl14d"] = 0.5 ** (
            daily["days_since_last_scored_speech"].astype(float)
            / float(half_life_days)
        )
        no_prior_score = daily[
            "days_since_last_scored_speech"
        ].isna()
        daily.loc[
            no_prior_score,
            "freshness_weight_hl14d",
        ] = pd.NA
        daily["freshness_adjusted_stance"] = (
            daily["stance_level_locf"]
            * daily["freshness_weight_hl14d"]
        )
        daily["is_score_fresh"] = (
            daily["days_since_last_scored_speech"].notna()
            & (daily["days_since_last_scored_speech"] <= fresh_days)
        ).astype("Int64")

        daily["date"] = daily["date"].dt.date.astype("string")
        daily["last_scored_speech_date"] = daily[
            "last_scored_speech_date"
        ].dt.date.astype("string")
        daily["updated_at"] = datetime.now().isoformat()

        columns = [
            "date",
            "bank_code",
            "daily_stance_score",
            "daily_scored_speech_count",
            "daily_total_speech_count",
            "stance_level_locf",
            "freshness_weight_hl14d",
            "freshness_adjusted_stance",
            "last_scored_speech_date",
            "days_since_last_scored_speech",
            "is_score_fresh",
            "has_scored_speech",
            "updated_at",
        ]
        return daily[columns]
