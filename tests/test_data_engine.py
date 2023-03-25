from hbsir.data_engine import load_table, add_attribute

df = load_table('food', 1400)

def test_add_attribute():
    assert len(add_attribute(df, year=1400, attribute=["Region"]).columns) == len(df.columns) + 1
