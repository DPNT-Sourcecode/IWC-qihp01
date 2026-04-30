from solutions.HLO.hello_solution import HelloSolution


class TestHello:
    def test_hello(self):
        assert HelloSolution().hello("Alice") == "Hello, Alice!"

    def test_hello_different_name(self):
        assert HelloSolution().hello("Bob") == "Hello, Bob!"
