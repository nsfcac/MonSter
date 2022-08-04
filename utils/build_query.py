from datetime import datetime


def build_query(table: str, start_time: datetime, finish_time: datetime, limit: int) -> str:
    """Builds SQL query using parameters.

    :param str table: table name.
    :param datetime start_time: query start time.
    :param datetime finish_time: query finish time.
    :param int limit: query limit.
    :return str: formatted SQL query.
    """
    query = f"SELECT * FROM {table}"

    if start_time or finish_time:
        filter_query = " WHERE"
        if start_time:
            filter_query += f" timestamp >= '{start_time}'"
        if finish_time:
            if start_time:
                filter_query += " AND"
            filter_query += f" timestamp < '{finish_time}'"
        
        query += filter_query

    query += " ORDER BY timestamp"

    if limit:
        query += f" LIMIT {limit}"

    return query
