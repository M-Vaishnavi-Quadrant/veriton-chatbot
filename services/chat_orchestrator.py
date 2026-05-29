import json
import re

from config import (
    AZURE_OPENAI_DEPLOYMENT
)


class ChatOrchestrator:

    def __init__(
        self,
        client
    ):

        self.client = client

    # =====================================================
    # NORMALIZE MESSAGE
    # =====================================================

    def normalize_message(
        self,
        message
    ):

        return (
            message
            .lower()
            .strip()
        )

    # =====================================================
    # RULE-BASED WORKFLOW DETECTION
    # =====================================================

    def detect_workflow_intent(
        self,
        message
    ):

        msg = self.normalize_message(
            message
        )

        # =================================================
        # DQ
        # =================================================

        dq_keywords = [

            "dq",
            "data quality",
            "apply dq",
            "quality rules",
            "validate quality",
            "run dq"
        ]

        if any(
            k in msg
            for k in dq_keywords
        ):

            return {

                "type": "execution",

                "intent": "dq",

                "confidence": 0.99,

                "entities": {

                    "workflow": "dq",

                    "datasets": []
                },

                "response": (
                    "Running Data Quality checks."
                )
            }

        # =================================================
        # NER
        # =================================================

        ner_keywords = [

            "ner",
            "run ner",
            "entity resolution",
            "named entity",
            "name entity",
            "detect entities"
        ]

        if any(
            k in msg
            for k in ner_keywords
        ):

            return {

                "type": "execution",

                "intent": "ner",

                "confidence": 0.99,

                "entities": {

                    "workflow": "ner",

                    "datasets": []
                },

                "response": (
                    "Running NER pipeline."
                )
            }

        # =================================================
        # BUSINESS LOGIC
        # =================================================

        business_logic_keywords = [

            "business logic",
            "apply business logic",
            "run business logic",
            "business rules",
            "apply rules",
            "run rules",
            "transform dataset"
        ]

        if any(
            k in msg
            for k in business_logic_keywords
        ):

            return {

                "type": "execution",

                "intent": "business_logic",

                "confidence": 0.99,

                "entities": {

                    "workflow": "business_logic",

                    "datasets": []
                },

                "response": (
                    "Applying business logic."
                )
            }

        # =================================================
        # DASHBOARD
        # =================================================

        dashboard_keywords = [

            "dashboard",
            "generate dashboard",
            "powerbi",
            "power bi",
            "create dashboard",
            "build dashboard"
        ]

        if any(
            k in msg
            for k in dashboard_keywords
        ):

            return {

                "type": "execution",

                "intent": "dashboard",

                "confidence": 0.99,

                "entities": {

                    "workflow": "dashboard",

                    "datasets": []
                },

                "response": (
                    "Generating dashboard."
                )
            }

        # =================================================
        # AUTOML
        # =================================================

        automl_keywords = [

            "automl",
            "auto ml",
            "build model",
            "train model",
            "machine learning",
            "run automl"
        ]

        if any(
            k in msg
            for k in automl_keywords
        ):

            return {

                "type": "execution",

                "intent": "automl",

                "confidence": 0.99,

                "entities": {

                    "workflow": "automl",

                    "datasets": []
                },

                "response": (
                    "Starting AutoML pipeline."
                )
            }

        # =================================================
        # PIPELINE
        # =================================================

        pipeline_keywords = [

            "pipeline",
            "schedule pipeline",
            "run pipeline"
        ]

        if any(
            k in msg
            for k in pipeline_keywords
        ):

            return {

                "type": "execution",

                "intent": "pipeline",

                "confidence": 0.99,

                "entities": {

                    "workflow": "pipeline",

                    "datasets": []
                },

                "response": (
                    "Starting pipeline."
                )
            }

        return None

    # =====================================================
    # DETECT INTENT
    # =====================================================

    def detect_intent(

        self,

        message,

        context
    ):

        # =================================================
        # RULE-BASED DETECTION FIRST
        # =================================================

        workflow_result = (
            self.detect_workflow_intent(
                message
            )
        )

        if workflow_result:

            return workflow_result

        # =================================================
        # LLM DETECTION
        # =================================================

        prompt = f"""
You are an enterprise AI orchestration engine.

Return ONLY valid JSON.

Possible intents:

1. conversation
2. etl
3. dq
4. ner
5. business_logic
6. dashboard
7. automl
8. pipeline

IMPORTANT:

- "apply business logic"
  -> business_logic

- "apply dq"
  -> dq

- "run ner"
  -> ner

- "generate dashboard"
  -> dashboard

- "build automl model"
  -> automl

- Use ETL ONLY when user is:
  - joining datasets
  - transforming datasets
  - building dataset
  - merging files
  - creating dataset
  - ingesting data

Message:
{message}

Current Context:
{json.dumps(context)}

FORMAT:
{{
  "type": "conversation | execution",
  "intent": "etl",
  "confidence": 0.98,
  "entities": {{
    "datasets": [],
    "workflow": "etl"
  }},
  "response": "..."
}}
"""

        response = (
            self.client.chat.completions.create(

                model=AZURE_OPENAI_DEPLOYMENT,

                messages=[

                    {
                        "role": "system",

                        "content": """
Return ONLY valid JSON.
Do not include markdown.
"""
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

        try:

            parsed = json.loads(raw)

        except Exception:

            parsed = {

                "type": "conversation",

                "intent": "conversation",

                "confidence": 0.5,

                "entities": {

                    "datasets": [],

                    "workflow": None
                },

                "response": raw
            }

        # =================================================
        # SAFETY FIX
        # =================================================

        if parsed.get("intent") == "etl":

            msg = self.normalize_message(
                message
            )

            dangerous_non_etl = [

                "business logic",
                "dq",
                "dashboard",
                "automl",
                "ner"
            ]

            if any(
                k in msg
                for k in dangerous_non_etl
            ):

                parsed["intent"] = "conversation"

        return parsed