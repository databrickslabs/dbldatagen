from .compat import BaseModel
from typing import Dict, Optional, Union, Any


# class ColumnDefinition(BaseModel):
#     name: str
#     type: Optional[DbldatagenBasicType] = None
#     primary: bool = False
#     options: Optional[Dict[str, Any]] = {}
#     nullable: Optional[bool] = False
#     omit: Optional[bool] = False
#     baseColumn: Optional[str] = "id"
#     baseColumnType: Optional[str] = "auto"

#     @model_validator(mode="after")
#     def check_constraints(self):
#         if self.primary:
#             if "min" in self.options or "max" in self.options:
#                 raise ValueError(
#                     f"Primary column '{self.name}' cannot have min/max options.")
#             if self.nullable:
#                 raise ValueError(
#                     f"Primary column '{self.name}' cannot be nullable.")
#             if self.primary and self.type is None:
#                 raise ValueError(
#                     f"Primary column '{self.name}' must have a type defined.")
#         return self


# class TableDefinition(BaseModel):
#     number_of_rows: int
#     partitions: Optional[int] = None
#     columns: List[ColumnDefinition]


# class DatagenSpec(BaseModel):
#     tables: Dict[str, TableDefinition]
#     output_destination: Optional[Union[UCSchemaTarget, FilePathTarget]] = None
#     generator_options: Optional[Dict[str, Any]] = {}
#     intended_for_databricks: Optional[bool] = None



#     def display_all_tables(self):
#         for table_name, table_def in self.tables.items():
#             print(f"Table: {table_name}")

#             if self.output_destination:
#                 output = f"{self.output_destination}"
#                 display(HTML(f"<strong>Output destination:</strong> {output}"))
#             else:
#                 message = (
#                     "<strong>Output destination:</strong> "
#                     "<span style='color: red; font-weight: bold;'>None</span><br>"
#                     "<span style='color: gray;'>Set it using the <code>output_destination</code> "
#                     "attribute on your <code>DatagenSpec</code> object "
#                     "(e.g., <code>my_spec.output_destination = UCSchemaTarget(...)</code>).</span>"
#                 )
#                 display(HTML(message))

#             df = pd.DataFrame([col.dict() for col in table_def.columns])
#             try:
#                 display(df)
#             except NameError:
#                 print(df.to_string())


class DatagenSpec(BaseModel):
    name: str