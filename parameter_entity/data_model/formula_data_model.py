from parameter_entity.data_model.manual_data_model import ManualDataModel


class FormulaDataModel(ManualDataModel):
    formula: str | None
    generate_count: int | None
