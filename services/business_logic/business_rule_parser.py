
# =====================================================
# FILE: services/business_logic/business_rule_parser.py
# =====================================================

import re


def parse_business_rule(rule_text: str):

    text = rule_text.lower().strip()

    parsed = {
        "type": "unknown",
        "target_column": None,
        "source_columns": [],
        "params": {}
    }

    # =====================================================
    # MULTIPLICATION
    # subtotal should equal quantity multiplied by unit_price
    # =====================================================

    if "multiplied by" in text:

        match = re.search(
            r"(.*?) should equal (.*?) multiplied by (.*)",
            text
        )

        if match:

            parsed["type"] = "multiplication"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["source_columns"] = [

                match.group(2).strip(),

                match.group(3).strip()
            ]

    # =====================================================
    # ADDITION
    # total should equal subtotal plus tax
    # =====================================================

    elif " plus " in text:

        match = re.search(
            r"(.*?) should equal (.*?) plus (.*)",
            text
        )

        if match:

            parsed["type"] = "addition"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["source_columns"] = [

                match.group(2).strip(),

                match.group(3).strip()
            ]

    # =====================================================
    # SUBTRACTION
    # profit should equal revenue minus cost
    # =====================================================

    elif " minus " in text:

        match = re.search(
            r"(.*?) should equal (.*?) minus (.*)",
            text
        )

        if match:

            parsed["type"] = "subtraction"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["source_columns"] = [

                match.group(2).strip(),

                match.group(3).strip()
            ]

    # =====================================================
    # DIVISION
    # margin should equal profit divided by revenue
    # =====================================================

    elif " divided by " in text:

        match = re.search(
            r"(.*?) should equal (.*?) divided by (.*)",
            text
        )

        if match:

            parsed["type"] = "division"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["source_columns"] = [

                match.group(2).strip(),

                match.group(3).strip()
            ]

    # =====================================================
    # CONCAT
    # create full_name using first_name and last_name
    # =====================================================

    elif "using" in text:

        match = re.search(
            r"create (.*?) using (.*?) and (.*)",
            text
        )

        if match:

            parsed["type"] = "concat"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["source_columns"] = [

                match.group(2).strip(),

                match.group(3).strip()
            ]

    # =====================================================
    # AGGREGATION
    # order_total should equal sum of subtotal grouped by order_id
    # =====================================================

    elif "sum of" in text and "grouped by" in text:

        match = re.search(
            r"(.*?) should equal sum of (.*?) grouped by (.*)",
            text
        )

        if match:

            parsed["type"] = "aggregation_sum"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["source_columns"] = [
                match.group(2).strip()
            ]

            parsed["params"]["group_by"] = (
                match.group(3).strip()
            )

    # =====================================================
    # CONDITIONAL
    # order_status should be 'Completed'
    # if transaction_status is 'Success'
    # =====================================================

    elif " if " in text and " is " in text:

        match = re.search(
            r"(.*?) should be '(.*?)' if (.*?) is '(.*?)'",
            rule_text,
            re.IGNORECASE
        )

        if match:

            parsed["type"] = "conditional"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["params"]["target_value"] = (
                match.group(2).strip()
            )

            parsed["params"]["condition_column"] = (
                match.group(3).strip()
            )

            parsed["params"]["condition_value"] = (
                match.group(4).strip()
            )

    # =====================================================
    # BUCKETING
    # customer_segment should be 'High Value'
    # if total_amount > 10000
    # =====================================================

    elif ">" in text and "should be" in text:

        match = re.search(
            r"(.*?) should be '(.*?)' if (.*?) > (.*)",
            rule_text,
            re.IGNORECASE
        )

        if match:

            parsed["type"] = "conditional_bucket"

            parsed["target_column"] = (
                match.group(1).strip()
            )

            parsed["params"]["target_value"] = (
                match.group(2).strip()
            )

            parsed["params"]["condition_column"] = (
                match.group(3).strip()
            )

            parsed["params"]["threshold"] = float(
                match.group(4).strip()
            )

    # =====================================================
    # DATE COMPARISON
    # join_date should not be after order_date
    # =====================================================

    elif "should not be after" in text:

        match = re.search(
            r"(.*?) should not be after (.*)",
            text
        )

        if match:

            parsed["type"] = "date_comparison"

            parsed["source_columns"] = [

                match.group(1).strip(),

                match.group(2).strip()
            ]

    # =====================================================
    # EXTRACT MONTH
    # extract month from order_date
    # =====================================================

    elif "extract month from" in text:

        match = re.search(
            r"extract month from (.*)",
            text
        )

        if match:

            parsed["type"] = "extract_month"

            parsed["source_columns"] = [
                match.group(1).strip()
            ]

    return parsed
