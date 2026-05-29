import re


def parse_rule(rule_text: str):

    text = rule_text.lower()

    parsed = {
        "type": "unknown",
        "column": None,
        "params": {}
    }

    # ==========================================
    # NOT NULL
    # ==========================================
    if "must not be null" in text:

        col = text.replace("must not be null", "").strip()

        parsed["type"] = "not_null"
        parsed["column"] = col

    # ==========================================
    # EMAIL
    # ==========================================
    elif "valid email" in text:

        col = text.replace("must be valid email", "").strip()

        parsed["type"] = "email"
        parsed["column"] = col

    # ==========================================
    # UNIQUE
    # ==========================================
    elif "must be unique" in text:

        col = text.replace("must be unique", "").strip()

        parsed["type"] = "unique"
        parsed["column"] = col

    # ==========================================
    # GREATER THAN
    # ==========================================
    elif "greater than" in text:

        match = re.search(
            r"(.*?) must be greater than (\d+)",
            text
        )

        if match:

            parsed["type"] = "range"

            parsed["column"] = match.group(1).strip()

            parsed["params"]["min"] = float(match.group(2))

    return parsed