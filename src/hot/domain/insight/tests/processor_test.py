from typing import Callable
import pytest
from pipeline.handler.processor import Processor
from pipeline.usecase.interactor_interface import InteractorInterface

class TestProcessor():

    class MockInteractor(InteractorInterface):
        def __init__(
                self, 
                load_silver_shift_table_func: Callable[[str, str], None] = None, 
                load_gold_shift_table_func: Callable[[str, str], None] = None
            ) -> None:
            self.load_silver_shift_table_count = 0
            self.load_gold_shift_table_count = 0

            self._load_silver_shift_table_func = load_silver_shift_table_func
            self._load_gold_shift_table_func = load_gold_shift_table_func

        def load_silver_shift_table(self, tenant_name: str, product_name: str) -> None:
            self.load_silver_shift_table_count += 1
            self._load_silver_shift_table_func(tenant_name, product_name)

        def load_gold_shiftfact_table(self, tenant_name: str, product_name: str) -> None:
            self.load_gold_shift_table_count += 1
            self._load_gold_shift_table_func(tenant_name, product_name)


class TestProcessor_handler(TestProcessor):
    def test_happy_silver_shift_path(self):
        expected_tenant_name = "tester"
        expected_product_name = "unittests"

        def load_silver_shift_table_func(tenant_name, product_name):
            assert tenant_name == expected_tenant_name
            assert product_name == expected_product_name

        mock_interactor = self.MockInteractor(
            load_silver_shift_table_func=load_silver_shift_table_func
        )
        
        processor = Processor(interactor=mock_interactor)
        
        processor.handler(
            tenant_name=expected_tenant_name, 
            product_name=expected_product_name, 
            table_name="shift", 
            data_layer="silver"
        )
        
        assert mock_interactor.load_silver_shift_table_count == 1
        assert mock_interactor.load_gold_shift_table_count == 0


    def test_happy_gold_shiftfact_path(self):
        expected_tenant_name = "tester"
        expected_product_name = "unittests"

        def load_gold_shift_table_count_func(tenant_name, product_name):
            assert tenant_name == expected_tenant_name
            assert product_name == expected_product_name

        mock_interactor = self.MockInteractor(
            load_gold_shift_table_func=load_gold_shift_table_count_func
        )
        
        processor = Processor(interactor=mock_interactor)
        
        processor.handler(
            tenant_name=expected_tenant_name, 
            product_name=expected_product_name, 
            table_name="shiftfact", 
            data_layer="gold"
        )
        
        assert mock_interactor.load_silver_shift_table_count == 0
        assert mock_interactor.load_gold_shift_table_count == 1


if __name__ == "__main__":
    pytest.main(verbosity=2)
