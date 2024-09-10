from parameter_entity.query.page_query import PageQuery


class ModelExperiment(PageQuery):
    experiment_name: str | None = None
    experiment_id: str | None = None
    publish_name: str | None = None
