import pandas as pd


# =====================================================
# FIXING ENGINE
# =====================================================

def run_dq_fixing(client, df, rules, proposed_solutions):

    fixed_df = df.copy()

    for solution in proposed_solutions:

        try:

            col = solution["column"]
            fix = solution["fix"]

            if col not in fixed_df.columns:
                continue

            # ==========================================
            # NULL FIX
            # ==========================================
            if fix == "fill_or_drop_nulls":

                if pd.api.types.is_numeric_dtype(
                    fixed_df[col]
                ):

                    fixed_df[col] = fixed_df[col].fillna(
                        fixed_df[col].median()
                    )

                else:

                    fixed_df[col] = fixed_df[col].fillna(
                        "unknown"
                    )

            # ==========================================
            # DUPLICATE FIX
            # ==========================================
            elif fix == "remove_duplicates":

                fixed_df = fixed_df.drop_duplicates(
                    subset=[col]
                )

            # ==========================================
            # EMAIL FIX
            # ==========================================
            elif fix == "replace_invalid_emails":

                fixed_df[col] = fixed_df[col].where(
                    fixed_df[col].astype(str).str.contains("@"),
                    "unknown@example.com"
                )

            # ==========================================
            # RANGE FIX
            # ==========================================
            elif fix == "clip_range":

                fixed_df[col] = fixed_df[col].clip(
                    lower=0
                )

            # ==========================================
            # DATE FIX
            # ==========================================
            elif fix == "replace_invalid_dates":

                fixed_df[col] = pd.to_datetime(
                    fixed_df[col],
                    errors="coerce"
                )

        except Exception as e:

            print(f"⚠️ Fix skipped: {str(e)}")

            continue

    return fixed_df
