from hbsir.utils import Argham


class TestParsing:
    def test_int(self):
        argham = Argham(2)

        assert 2 in argham
        assert 4 not in argham

    def test_list(self):
        argham = Argham([2, 4, 5, 6, 6, 9, 11])

        assert 2 in argham
        assert 5 in argham
        assert 6 in argham
        assert 1 not in argham
        assert 7 not in argham
        assert 13 not in argham

    def test_simple_dict(self):
        argham = Argham({"a": 12, 1: "b", 3: 4, "c": [8, 9, 10]})

        assert 12 in argham
        assert 1 not in argham
        assert 3 not in argham
        assert 4 in argham
        assert 5 not in argham
        assert 8 in argham
        assert 9 in argham
        assert 10 in argham

    def test_range_dict(self):
        argham = Argham({"start": 1000, "end": 2000})

        assert 500 not in argham
        assert 1000 in argham
        assert 1500 in argham
        assert 1999 in argham
        assert 2000 not in argham
        assert 2500 not in argham

    def test_default_start(self):
        argham = Argham({"end": 2000}, default_start=1000)

        assert 500 not in argham
        assert 1000 in argham
        assert 1500 in argham
        assert 1999 in argham
        assert 2000 not in argham
        assert 2500 not in argham

    def test_default_end(self):
        argham = Argham({"start": 1000}, default_end=2000)

        assert 500 not in argham
        assert 1000 in argham
        assert 1500 in argham
        assert 1999 in argham
        assert 2000 not in argham
        assert 2500 not in argham

    def test_step(self):
        argham = Argham({"start": 1000, "end": 2000, "step": 5})

        assert 500 not in argham
        assert 1000 in argham
        assert 1234 not in argham
        assert 1500 in argham
        assert 1999 not in argham
        assert 2000 not in argham
        assert 2500 not in argham

    def test_default_step(self):
        argham = Argham({"start": 1000, "end": 2000}, default_step=5)

        assert 500 not in argham
        assert 1000 in argham
        assert 1234 not in argham
        assert 1500 in argham
        assert 1999 not in argham
        assert 2000 not in argham
        assert 2500 not in argham


class TestEquality:
    def test_equality_with_it_self(self):
        argham = Argham([1, 2, 3, {"start": 10, "end": 20, "step": 2}])

        assert argham == argham

    def test_sample_1(self):
        argham_1 = Argham([1, 2, 3, 4, 5, 6, 7, 8, 9])
        argham_2 = Argham({"start": 1, "end": 10})

        assert argham_1 == argham_2


class TestAdd:
    def test_integer_addition(self):
        argham_1 = Argham([1, 2, 3, 4])
        argham_2 = Argham(6)
        argham_3 = Argham([1, 2, 3, 4, 6])

        assert argham_1 + argham_2 == argham_3

    def test_range_addition(self):
        argham_1 = Argham({"start": 1, "end": 10})
        argham_2 = Argham({"start": 10, "end": 20})
        argham_3 = Argham({"start": 1, "end": 20})

        assert argham_1 + argham_2 == argham_3
