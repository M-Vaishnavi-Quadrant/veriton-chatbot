# =====================================================
# FILE: services/business_logic/business_logic_service.py
# =====================================================

from io import BytesIO
import json
import pandas as pd

from azure.storage.blob import BlobServiceClient

from config import (
    V_CONNECTION_STRING,
    V_DATASET_CONTAINER,
    AZURE_OPENAI_DEPLOYMENT
)

from services.business_logic.business_rule_parser import (
    parse_business_rule
)


class BusinessLogicService:

    def __init__(self, client):

        self.client = client

        self.blob_service = (
            BlobServiceClient
            .from_connection_string(
                V_CONNECTION_STRING
            )
        )

    # =====================================================
    # LOAD DATASET
    # =====================================================

    def load_dataset(self, blob_path):

        blob_client = (
            self.blob_service
            .get_blob_client(
                container=V_DATASET_CONTAINER,
                blob=blob_path
            )
        )

        if not blob_client.exists():

            raise Exception(
                f"Dataset not found: {blob_path}"
            )

        data = (
            blob_client
            .download_blob()
            .readall()
        )

        # ==============================================
        # AUTO DETECT DELIMITER
        # ==============================================


        # ==============================================
        # TRY MULTIPLE HEADER ROWS
        # ==============================================

        best_df = None

        best_score = -1

        for header_row in range(5):

            try:

                temp_df = pd.read_csv(

                    BytesIO(data),

                    sep=None,

                    engine="python",

                    header=header_row
                )

                # ==========================================
                # CLEAN COLUMNS
                # ==========================================

                temp_df.columns = [

                    str(c)
                    .strip()
                    .lower()
                    .replace(" ", "_")

                    for c in temp_df.columns
                ]

                # ==========================================
                # REMOVE UNNAMED
                # ==========================================

                temp_df = temp_df.loc[
                    :,
                    ~temp_df.columns.astype(str)
                    .str.contains("^unnamed")
                ]

                # ==========================================
                # HEADER QUALITY SCORE
                # ==========================================

                score = 0

                for col in temp_df.columns:

                    col = str(col)

                    # longer meaningful columns
                    if len(col) > 3:
                        score += 1

                    # numeric-like columns are bad
                    if col.isnumeric():
                        score -= 2

                    # common business keywords
                    business_keywords = [

                        "sales",
                        "revenue",
                        "profit",
                        "cost",
                        "amount",
                        "date",
                        "customer",
                        "product",
                        "quantity",
                        "price",
                        "total",
                        "margin",
                        "income",
                        "expense"
                    ]

                    if any(
                        k in col
                        for k in business_keywords
                    ):
                        score += 3

                    # title-like words are bad
                    bad_words = [

                        "statement",
                        "consolidated",
                        "figures",
                        "lakhs",
                        "fy"
                    ]

                    if any(
                        b in col
                        for b in bad_words
                    ):
                        score -= 3

                print(
                    f"\n🧪 Header Row {header_row}"
                )

                print(
                    "Columns:",
                    list(temp_df.columns)
                )

                print(
                    "Score:",
                    score
                )

                if score > best_score:

                    best_score = score

                    best_df = temp_df

            except Exception as e:

                print(
                    f"⚠️ Header parse failed "
                    f"for row {header_row}: {str(e)}"
                )

        # ==============================================
        # FINAL DATAFRAME
        # ==============================================

        df = best_df

        print("\n✅ FINAL DATASET INFO")
        print("Columns:", list(df.columns))
        print("Shape:", df.shape)
        print("Header Score:", best_score)


        return df, blob_client

    # =====================================================
    # SAVE DATASET
    # =====================================================

    def save_dataset(self, df, blob_client):

        output = BytesIO()

        df.to_csv(
            output,
            index=False
        )

        output.seek(0)

        blob_client.upload_blob(
            output.getvalue(),
            overwrite=True
        )

    # =====================================================
    # VALIDATE RULES
    # =====================================================

    def validate_business_rules(self, rules):

        supported = []

        for rule in rules:

            try:

                parsed = parse_business_rule(
                    rule["rule"]
                )
                print("\n🔍 VALIDATING RULE")
                print(rule["rule"])
                print("Parsed:", parsed)

                if parsed["type"] != "unknown":

                    supported.append(rule)

            except Exception as e:

                print(
                    "⚠️ Rule validation failed:",
                    str(e)
                )

                continue

        return supported

    # =====================================================
    # GENERATE AI RULES
    # =====================================================

    def generate_rules(self, df):

        columns = list(df.columns)

        sample = (
            df.head(5)
            .fillna("")
            .to_dict(orient="records")
        )

        print("\n🤖 GENERATING BUSINESS RULES")
        print("Columns:", columns)


        prompt = f"""
        You are an enterprise business analyst AI.

        Analyze the dataset columns and generate
        practical business transformation rules.

        IMPORTANT:
        - ONLY use EXISTING columns.
        - NEVER invent columns.
        - Generate ONLY meaningful rules.
        - Generate at least 5 rules if applicable.

        Dataset Columns:
        {columns}

        Sample Data:
        {json.dumps(sample, indent=2)}

        SUPPORTED RULE TYPES:

        1. Multiplication
        Example:
        subtotal should equal quantity multiplied by unit_price

        2. Addition
        Example:
        total should equal subtotal plus tax

        3. Subtraction
        Example:
        profit should equal revenue minus cost

        4. Division
        Example:
        margin should equal profit divided by revenue

        5. Concatenation
        Example:
        create customer_full_name using customer_first_name and customer_last_name

        6. Aggregation
        Example:
        order_total should equal sum of subtotal grouped by order_id

        7. Conditional
        Example:
        order_status should be 'Completed' if transaction_status is 'Success'

        8. Date comparison
        Example:
        join_date should not be after order_date

        9. Date feature extraction
        Example:
        extract month from order_date

        10. Conditional bucket
        Example:
        customer_segment should be 'High Value' if order_total_amount > 10000

        RULE GENERATION GUIDELINES:

        - If quantity and unit_price exist:
        generate subtotal calculation

        - If first_name and last_name exist:
        generate full name column

        - If order_date or transaction_date exists:
        generate month extraction

        - If subtotal and order_id exist:
        generate aggregation rule

        - If transaction_status exists:
        generate conditional business status

        - If numeric transaction/order columns exist:
        generate bucketization rules

        Return ONLY valid JSON.

        FORMAT:
        [
        {{
            "rule": "subtotal should equal quantity multiplied by unit_price"
        }}
        ]
        """




        response = (
            self.client.chat.completions.create(

                model=AZURE_OPENAI_DEPLOYMENT,

                messages=[

                    {
                        "role": "system",

                        "content": (
                            "Return ONLY valid JSON."
                        )
                    },

                    {
                        "role": "user",

                        "content": prompt
                    }
                ],

                temperature=0
            )
        )

        raw = (
            response
            .choices[0]
            .message
            .content
        )

        raw = raw.replace(
            "```json",
            ""
        )

        raw = raw.replace(
            "```",
            ""
        )

        raw = raw.strip()

        print("\n🤖 RAW AI RULES")
        print(raw)

        try:

            rules = json.loads(raw)

        except Exception as e:

            print(
                "❌ JSON PARSE ERROR:",
                str(e)
            )

            rules = []

        normalized_rules = []

        for r in rules:

            try:

                rule_text = (
                    r.get("rule", "")
                    .strip()
                )

                # ==========================================
                # NORMALIZE SYMBOLS
                # ==========================================

                rule_text = (
                    rule_text
                    .replace("*", " multiplied by ")
                    .replace("+", " plus ")
                    .replace("-", " minus ")
                    .replace("/", " divided by ")
                )

                # ==========================================
                # NORMALIZE EQUALS
                # ==========================================

                if "=" in rule_text:

                    parts = rule_text.split("=")

                    if len(parts) == 2:

                        left = parts[0].strip()

                        right = parts[1].strip()

                        rule_text = (
                            f"{left} should equal {right}"
                        )

                normalized_rules.append({

                    "rule": rule_text
                })

            except Exception as e:

                print(
                    "⚠️ Rule normalization failed:",
                    str(e)
                )

        validated_rules = (
            self.validate_business_rules(
                normalized_rules
            )
        )

        return validated_rules

    # =====================================================
    # APPLY RULES
    # =====================================================

    def apply_rules(self, df, rules):

        transformed_df = df.copy()

        applied_rules = []

        skipped_rules = []

        generated_columns = []

        for rule_obj in rules:

            try:

                rule_text = rule_obj["rule"]

                print(
                    f"\n⚙️ Applying Rule: {rule_text}"
                )

                parsed = parse_business_rule(
                    rule_text
                )

                print("Parsed:", parsed)

                rule_type = parsed["type"]

                missing_cols = [

                    c for c in parsed["source_columns"]

                    if c not in transformed_df.columns
                ]

                if missing_cols:

                    skipped_rules.append({

                        "rule": rule_text,

                        "error": (
                            f"Missing columns: "
                            f"{missing_cols}"
                        )
                    })

                    continue

                # =============================================
                # MULTIPLICATION
                # =============================================

                if rule_type == "multiplication":

                    target = parsed["target_column"]

                    c1, c2 = parsed["source_columns"]

                    transformed_df[target] = (

                        pd.to_numeric(
                            transformed_df[c1],
                            errors="coerce"
                        ).fillna(0)

                        *

                        pd.to_numeric(
                            transformed_df[c2],
                            errors="coerce"
                        ).fillna(0)
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # ADDITION
                # =============================================

                elif rule_type == "addition":

                    target = parsed["target_column"]

                    c1, c2 = parsed["source_columns"]

                    transformed_df[target] = (

                        pd.to_numeric(
                            transformed_df[c1],
                            errors="coerce"
                        ).fillna(0)

                        +

                        pd.to_numeric(
                            transformed_df[c2],
                            errors="coerce"
                        ).fillna(0)
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # SUBTRACTION
                # =============================================

                elif rule_type == "subtraction":

                    target = parsed["target_column"]

                    c1, c2 = parsed["source_columns"]

                    transformed_df[target] = (

                        pd.to_numeric(
                            transformed_df[c1],
                            errors="coerce"
                        ).fillna(0)

                        -

                        pd.to_numeric(
                            transformed_df[c2],
                            errors="coerce"
                        ).fillna(0)
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # DIVISION
                # =============================================

                elif rule_type == "division":

                    target = parsed["target_column"]

                    c1, c2 = parsed["source_columns"]

                    denominator = pd.to_numeric(
                        transformed_df[c2],
                        errors="coerce"
                    ).replace(0, 1)

                    transformed_df[target] = (

                        pd.to_numeric(
                            transformed_df[c1],
                            errors="coerce"
                        ).fillna(0)

                        / denominator
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # CONCAT
                # =============================================

                elif rule_type == "concat":

                    target = parsed["target_column"]

                    c1, c2 = parsed["source_columns"]

                    transformed_df[target] = (

                        transformed_df[c1]
                        .astype(str)

                        + " "

                        +

                        transformed_df[c2]
                        .astype(str)
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # AGGREGATION
                # =============================================

                elif rule_type == "aggregation_sum":

                    target = parsed["target_column"]

                    source = parsed[
                        "source_columns"
                    ][0]

                    group_by = parsed[
                        "params"
                    ]["group_by"]

                    transformed_df[target] = (

                        transformed_df
                        .groupby(group_by)[source]
                        .transform("sum")
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # CONDITIONAL
                # =============================================

                elif rule_type == "conditional":

                    target = parsed[
                        "target_column"
                    ]

                    condition_col = parsed[
                        "params"
                    ]["condition_column"]

                    condition_val = parsed[
                        "params"
                    ]["condition_value"]

                    target_val = parsed[
                        "params"
                    ]["target_value"]

                    transformed_df[target] = (

                        transformed_df[
                            condition_col
                        ]
                        .astype(str)
                        .apply(

                            lambda x:

                            target_val

                            if x == condition_val

                            else None
                        )
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # CONDITIONAL BUCKET
                # =============================================

                elif rule_type == "conditional_bucket":

                    target = parsed[
                        "target_column"
                    ]

                    condition_col = parsed[
                        "params"
                    ]["condition_column"]

                    threshold = parsed[
                        "params"
                    ]["threshold"]

                    target_val = parsed[
                        "params"
                    ]["target_value"]

                    transformed_df[target] = (

                        pd.to_numeric(
                            transformed_df[
                                condition_col
                            ],
                            errors="coerce"
                        )

                        .apply(

                            lambda x:

                            target_val

                            if x > threshold

                            else "Standard"
                        )
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # DATE COMPARISON
                # =============================================

                elif rule_type == "date_comparison":

                    d1, d2 = parsed[
                        "source_columns"
                    ]

                    transformed_df[d1] = pd.to_datetime(

                        transformed_df[d1],

                        errors="coerce",

                        format="mixed"
                    )

                    transformed_df[d2] = pd.to_datetime(

                        transformed_df[d2],

                        errors="coerce",

                        format="mixed"
                    )

                    target = (
                        f"{d1}_valid"
                    )

                    transformed_df[target] = (

                        transformed_df[d1]
                        <= transformed_df[d2]
                    )

                    generated_columns.append(
                        target
                    )

                # =============================================
                # EXTRACT MONTH
                # =============================================

                elif rule_type == "extract_month":

                    source = parsed[
                        "source_columns"
                    ][0]

                    target = (
                        f"{source}_month"
                    )

                    transformed_df[source] = (
                        pd.to_datetime(
                            transformed_df[source],
                            errors="coerce",
                            format="mixed"
                        )
                    )

                    transformed_df[target] = (
                        transformed_df[source]
                        .dt.month
                    )

                    generated_columns.append(
                        target
                    )

                applied_rules.append(
                    rule_text
                )

            except Exception as e:

                print(
                    "❌ RULE ERROR:",
                    str(e)
                )

                skipped_rules.append({

                    "rule": rule_obj.get("rule"),

                    "error": str(e)
                })

        return (

            transformed_df,

            applied_rules,

            skipped_rules,

            generated_columns
        )

    # =====================================================
    # RUN BUSINESS LOGIC
    # =====================================================

    def run(self, blob_path, rules=None):

        df, blob_client = self.load_dataset(
            blob_path
        )

        # ==============================================
        # AUTO GENERATE RULES
        # ==============================================

        if rules is None:

            rules = self.generate_rules(df)

        (
            transformed_df,
            applied_rules,
            skipped_rules,
            generated_columns
        ) = self.apply_rules(
            df,
            rules
        )

        # ==============================================
        # NO RULES GENERATED
        # ==============================================

        if not rules:

            return {

                "status": "warning",

                "blob_path": blob_path,

                "message": (

                    "Business logic rules could not "

                    "be generated for this dataset."
                ),

                "reason": (

                    "Dataset structure is insufficient "

                    "for business transformations."
                ),

                "details": {

                    "columns_detected": list(df.columns),

                    "column_count": len(df.columns),

                    "rows": len(df)
                },

                "suggestions": [

                    "Upload structured tabular datasets",

                    "Include numeric and categorical columns",

                    "Include revenue, sales, customer, "

                    "date, or transaction-related fields"
                ],

                "next_actions": [

                    {
                        "action": "dashboard",
                        "label": "Generate Dashboard"
                    },

                    {
                        "action": "automl",
                        "label": "Build AutoML Model"
                    }
                ]
            }


        # ==============================================
        # SAVE DATASET
        # ==============================================

        self.save_dataset(
            transformed_df,
            blob_client
        )

        return {

            "status": "success",

            "blob_path": blob_path,

            "rules_received": len(rules),

            "rules_applied": len(
                applied_rules
            ),

            "rules_skipped": skipped_rules,

            "applied_rules": applied_rules,

            "generated_rules": rules,

            "generated_columns": (
                generated_columns
            ),

            "rows": len(
                transformed_df
            ),

            "columns": len(
                transformed_df.columns
            ),

            "next_actions": [

                {
                    "action": "dashboard",
                    "label": "Generate Dashboard"
                },

                {
                    "action": "automl",
                    "label": "Build AutoML Model"
                }
            ]
        }

