from parameter_entity.query.page_query import PageQuery


class DataTable(PageQuery):
    datasource_id: str | None
    table_name: str | None = None
