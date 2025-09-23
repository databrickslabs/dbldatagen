from dbldatagen.spec.generator import DatagenSpec

def test_spec():
    spec = DatagenSpec(name="test_spec")
    assert spec.name == "test_spec"


