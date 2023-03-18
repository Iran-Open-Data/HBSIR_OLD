import hbsir.utils as utils


def test_build_year_interval():
    assert utils.build_year_interval(1396, 1400) == (1396, 1401)
