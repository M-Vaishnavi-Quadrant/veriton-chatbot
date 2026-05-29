import re
import pandas as pd

from services.DQ_rules.rule_parser import parse_rule

# =====================================================
# EMAIL VALIDATION
# =====================================================

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


# =====================================================
# VALIDATION ENGINE
# =====================================================

def run_dq_validation(client, df, rules):

    issues = []
    proposed_solutions = []

    total_cols = len(df.columns)

    for rule_obj in rules:

        try:

            rule_text = rule_obj["rule"]

            parsed = parse_rule(rule_text)

            rule_type = parsed["type"]
            col = parsed["column"]
            params = parsed.get("params", {})

            if not col:
                continue

            if col not in df.columns:
                continue

            # ==========================================
            # NOT NULL
            # ==========================================
            if rule_type == "not_null":

                null_count = df[col].isna().sum()

                if null_count > 0:

                    issues.append({
                        "column": col,
                        "rule": rule_text,
                        "issue": f"{null_count} null values found"
                    })

                    proposed_solutions.append({
                        "column": col,
                        "fix": "fill_or_drop_nulls"
                    })

            # ==========================================
            # UNIQUE
            # ==========================================
            elif rule_type == "unique":

                dup_count = df[col].duplicated().sum()

                if dup_count > 0:

                    issues.append({
                        "column": col,
                        "rule": rule_text,
                        "issue": f"{dup_count} duplicate values found"
                    })

                    proposed_solutions.append({
                        "column": col,
                        "fix": "remove_duplicates"
                    })

            # ==========================================
            # EMAIL
            # ==========================================
            elif rule_type == "email":

                invalid = (
                    ~df[col]
                    .astype(str)
                    .str.match(EMAIL_REGEX, na=False)
                )

                invalid_count = invalid.sum()

                if invalid_count > 0:

                    issues.append({
                        "column": col,
                        "rule": rule_text,
                        "issue": f"{invalid_count} invalid emails found"
                    })

                    proposed_solutions.append({
                        "column": col,
                        "fix": "replace_invalid_emails"
                    })

            # ==========================================
            # RANGE
            # ==========================================
            elif rule_type == "range":

                min_val = params.get("min")

                if min_val is not None:

                    invalid = df[col] < min_val

                    invalid_count = invalid.sum()

                    if invalid_count > 0:

                        issues.append({
                            "column": col,
                            "rule": rule_text,
                            "issue": f"{invalid_count} values below {min_val}"
                        })

                        proposed_solutions.append({
                            "column": col,
                            "fix": "clip_range"
                        })

            # ==========================================
            # DATETIME
            # ==========================================
            elif rule_type == "datetime":

                invalid_dates = pd.to_datetime(
                    df[col],
                    errors="coerce"
                ).isna()

                invalid_count = invalid_dates.sum()

                if invalid_count > 0:

                    issues.append({
                        "column": col,
                        "rule": rule_text,
                        "issue": f"{invalid_count} invalid dates found"
                    })

                    proposed_solutions.append({
                        "column": col,
                        "fix": "replace_invalid_dates"
                    })

        except Exception as e:

            print(f"⚠️ Validation skipped: {str(e)}")

            continue

    return (
        issues,
        total_cols,
        proposed_solutions
    )