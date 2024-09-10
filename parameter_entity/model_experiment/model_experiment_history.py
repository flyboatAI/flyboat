from parameter_entity.query.page_query import PageQuery


class ModelExperimentHistory(PageQuery):
    experiment_id: str | None
    execute_id: str | None = None
