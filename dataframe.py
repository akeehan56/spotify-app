import re

class DataFrame:
    def __init__(self, data):
        self.data = data
        self.columns = list(data.keys())
        self.num_rows = len(next(iter(data.values()))) if data else 0

    def __getitem__(self, key):
        """Get a column by name."""
        return self.data[key]

    def __repr__(self):
        """Pretty print a preview of the DataFrame (first 5 rows)."""
        preview_rows = min(5, self.num_rows)
        preview = {col: self.data[col][:preview_rows] for col in self.columns}
        return f"DataFrame({preview})"

    def row(self, index):
        """Return a single row as a dict."""
        if index < 0 or index >= self.num_rows:
            raise IndexError("Row index out of range")
        return {col: self.data[col][index] for col in self.columns}

    def filter(self, func):
        """Filter rows based on a condition (function returns True/False)."""
        filtered_data = {col: [] for col in self.columns}
        for i in range(self.num_rows):
            row = {col: self.data[col][i] for col in self.columns}
            if func(row):
                for col in self.columns:
                    filtered_data[col].append(self.data[col][i])
        return DataFrame(filtered_data)

    def select(self, cols):
        """Select a subset of columns."""
        selected_data = {col: self.data[col] for col in cols}
        return DataFrame(selected_data)

    def group_by(self, group_col):
        """Group the DataFrame by a column, returning a dict of DataFrames."""
        groups = {}
        for i in range(self.num_rows):
            key = self.data[group_col][i]
            if key not in groups:
                groups[key] = {col: [] for col in self.columns}
            for col in self.columns:
                groups[key][col].append(self.data[col][i])
        return {k: DataFrame(v) for k, v in groups.items()}

    def aggregate(self, col, func):
        """Apply an aggregate function to a single column."""
        return func(self.data[col])


# --- Helper functions ---
def convert_value(val):
    """Try to convert strings to int or float, otherwise keep as str."""
    if val == "":
        return val
    if len(val) >= 2 and val[0] == '"' and val[-1] == '"':
        val = val[1:-1]
    try:
        return int(val)
    except ValueError:
        try:
            return float(val)
        except ValueError:
            return val


def read_csv(filename, sep=","):
    """Read a CSV file into a DataFrame (handles quotes and numeric types)."""
    pattern = re.compile(
        r'''
        \s*
        (?:
          "((?:[^"]|"")*)"   # Quoted field, handle escaped quotes
          |
          ([^",]*)           # Unquoted field
        )
        \s*
        (?:,|$)
        ''',
        re.VERBOSE
    )

    columns = None
    with open(filename, encoding="utf-8-sig") as f:
        for i, line in enumerate(f):
            line = line.rstrip("\n\r")
            if not line:
                continue

            values = []
            for m in pattern.finditer(line):
                val = m.group(1) or m.group(2) or ""
                val = val.replace('""', '"').strip()
                val = convert_value(val)
                values.append(val)

            if i == 0:
                header = values
                columns = {h: [] for h in header}
            else:
                for h, v in zip(header, values):
                    columns[h].append(v)

    return DataFrame(columns)