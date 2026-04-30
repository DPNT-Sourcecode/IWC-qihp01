from solutions.HLO.hello_solution import HelloSolution


class TestHello:
    def test_hello(self):
        assert HelloSolution().hello("Craftsman") == "Hello, World!"

    def test_hello_different_name(self):
        assert HelloSolution().hello("Mr. X") == "Hello, World!"

