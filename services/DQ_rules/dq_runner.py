from services.DQ_rules.dq_validation import run_dq_validation
from services.DQ_rules.dq_fixing import run_dq_fixing

def run_dq_pipeline(client, df, rules):

    # =====================================================
    # STEP 1: VALIDATION
    # =====================================================
    issues, total_cols, proposed_solutions = run_dq_validation(
        client,
        df,
        rules
    )

    # =====================================================
    # STEP 2: FIXING
    # =====================================================
    fixed_df = run_dq_fixing(
        client,
        df,
        rules,
        proposed_solutions
    )

    # =====================================================
    # STEP 3: REVALIDATION
    # =====================================================
    reissues, _, _ = run_dq_validation(
        client,
        fixed_df,
        rules
    )

    return {
        "fixed_df": fixed_df,
        "issues_before": issues,
        "issues_after": reissues,
        "proposed_solutions": proposed_solutions,
        "columns_checked": total_cols
    }