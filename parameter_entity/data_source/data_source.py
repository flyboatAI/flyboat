from parameter_entity.data_source.test_data_source_link import TestDataSourceLink


class DataSource(TestDataSourceLink):
    datasource_name: str | None
    db_type: str | None
    db_description: str | None = None
    datasource_id: str | None = None
