from collections import defaultdict


class ModelBuilder:

    def __init__(self, etl_output):
        self.columns = etl_output.get("columns", [])
        self.lineage = etl_output.get("lineage", {})
        self.preview = etl_output.get("preview", [])
        self.rows = etl_output.get("rows", 0)

    # --------------------------------------------------
    # 1. TABLE → COLUMN GROUPING
    # --------------------------------------------------
    def get_table_columns(self):
        table_cols = defaultdict(list)

        for col in self.columns:
            table = self.lineage.get(col, "unknown")
            table_cols[table].append(col)

        # remove unknown if empty
        return {k: v for k, v in table_cols.items() if k != "unknown"}

    # --------------------------------------------------
    # 2. FACT TABLE (DUPLICATION BASED)
    # --------------------------------------------------
    def detect_fact_table(self, table_cols):

        fact_scores = {}

        for table, cols in table_cols.items():

            if not cols:
                continue

            key_col = cols[0]

            try:
                values = [row.get(key_col) for row in self.preview if key_col in row]
                unique_vals = len(set(values))

                if unique_vals == 0:
                    continue

                duplication_ratio = len(values) / unique_vals

                fact_scores[table] = duplication_ratio

            except:
                continue

        if fact_scores:
            return max(fact_scores, key=fact_scores.get)

        # fallback
        return list(table_cols.keys())[0]

    # --------------------------------------------------
    # 3. DIMENSIONS
    # --------------------------------------------------
    def detect_dimensions(self, table_cols, fact):
        return [t for t in table_cols if t != fact]

    # --------------------------------------------------
    # 4. RELATIONSHIPS
    # --------------------------------------------------
    def build_relationships(self, fact, dims, table_cols):

        relationships = []

        for dim in dims:

            fact_cols = table_cols.get(fact, [])
            dim_cols = table_cols.get(dim, [])

            best_match = None
            best_score = 0

            for f_col in fact_cols:
                for d_col in dim_cols:

                    # Step 1: name similarity
                    if f_col.lower() != d_col.lower():
                        continue

                    # Step 2: value overlap
                    try:
                        f_values = set([
                            row.get(f_col)
                            for row in self.preview
                            if f_col in row
                        ])

                        d_values = set([
                            row.get(d_col)
                            for row in self.preview
                            if d_col in row
                        ])

                        if not f_values or not d_values:
                            continue

                        overlap = len(f_values & d_values)
                        score = overlap / max(len(f_values), 1)

                        if score > best_score:
                            best_score = score
                            best_match = (f_col, d_col)

                    except:
                        continue

            if best_match:
                relationships.append({
                    "from": fact,
                    "to": dim,
                    "join": f"{best_match[0]} = {best_match[1]}"
                })
            else:
                relationships.append({
                    "from": fact,
                    "to": dim,
                    "join": "auto (no strong key found)"
                })

        return relationships

    # --------------------------------------------------
    # 5. SCHEMA
    # --------------------------------------------------
    def build_schema(self, table_cols):
        return dict(table_cols)

    # --------------------------------------------------
    # FINAL BUILD
    # --------------------------------------------------
    def build(self):

        table_cols = self.get_table_columns()

        if not table_cols:
            return {
                "fact_table": None,
                "dimension_tables": [],
                "relationships": [],
                "schemas": {}
            }

        fact = self.detect_fact_table(table_cols)
        dims = self.detect_dimensions(table_cols, fact)
        rels = self.build_relationships(fact, dims, table_cols)
        schema = self.build_schema(table_cols)

        return {
            "fact_table": fact,
            "dimension_tables": dims,
            "relationships": rels,
            "schemas": schema
        }