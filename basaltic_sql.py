"""
Jupyter cell magic `%%basaltic_sql` for basaltic-red.

Pure-Python; register from a notebook with:

    from basaltic_sql import load_ipython_extension
    load_ipython_extension(get_ipython())

Usage:

    %%basaltic_sql <var_name> [--path PATH]
    SELECT passenger_count, COUNT(*) AS n FROM '{path}'

Body is raw SQL run through `br.sql.execute_sql_stream`; `{var}` placeholders
interpolate from the notebook namespace (plus `{path}` from `--path`). Result
becomes a Polars DataFrame (pyarrow Table fallback), is assigned to var_name
and displayed in the cell.
"""

import basaltic_red as br


def load_ipython_extension(ipython):
    from IPython.core.magic import Magics, cell_magic, magics_class, needs_local_scope

    @magics_class
    class BasalticSQL(Magics):
        @cell_magic
        @needs_local_scope
        def basaltic_sql(self, line, cell, local_ns):
            var_name = None
            path = None
            args = line.split()
            i = 0
            while i < len(args):
                if args[i] == "--path" and i + 1 < len(args):
                    path = args[i + 1]
                    i += 2
                else:
                    var_name = args[i]
                    i += 1

            ns = dict(self.shell.user_ns)
            ns.update(local_ns)
            if path is not None:
                ns["path"] = path
            sql = cell.format(**ns)

            stream = br.sql.execute_sql_stream(sql)
            try:
                result = stream.to_polars()
            except Exception:
                result = stream.to_pyarrow()

            if var_name:
                self.shell.user_ns[var_name] = result
            return result

    ipython.register_magics(BasalticSQL)
