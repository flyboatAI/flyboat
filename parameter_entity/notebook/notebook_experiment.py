from parameter_entity.query.page_query import PageQuery


class NotebookExperiment(PageQuery):
    experiment_name: str | None = None
    experiment_id: str | None = None
