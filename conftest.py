from hbsir import data_engine

def pytest_configure(config):
    print('Basic initialization of HBSIR')
    try:
        data_engine.load_table('food', 1400)
        print('Successful basic initialization of HBSIR')
    except MyError:
        pytest.fail("Failed basic initialization")
