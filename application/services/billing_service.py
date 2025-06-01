from typing import Any, Tuple
from application.ports.out.repository_port import RepositoryPort
# La conversión de `productos_json_str` a `Json` (para PostgreSQL) o mantenerlo como `str`
# debe ser manejada por la implementación del `RepositoryPort` (DbRepository) o, idealmente,
# por el método específico del conector (`generate_invoice` o `execute_sp`).

class BillingService:
    """
    Servicio de aplicación para la lógica de facturación.
    """
    def __init__(self, repository: RepositoryPort):
        self.repository = repository

    def generate_invoice_process(self, client_id: int, staff_id: int, products_json_str: str) -> Tuple[Any, float]:
        """
        Procesa la generación de una factura.

        Args:
            client_id (int): ID del cliente.
            staff_id (int): ID del personal/vendedor.
            products_json_str (str): Cadena JSON con los productos y cantidades.
                                     Ej: '[{"producto_id": 1, "cantidad": 2}]'

        Returns:
            Tuple[Any, float]: El resultado de la ejecución del SP (si lo hay) y el tiempo de ejecución en ms.
        """
        try:
            # El método `generate_invoice` del RepositoryPort ya está diseñado para
            # medir el tiempo y llamar al método subyacente del conector que, a su vez,
            # llama al stored procedure 'sp_generar_factura'.
            # Los parámetros coinciden con los de `RepositoryPort.generate_invoice`.
            
            result, exec_time = self.repository.generate_invoice(
                client_id=client_id,
                staff_id=staff_id,
                products_json_str=products_json_str
            )
            print(f"BillingService: Factura generada. Tiempo: {exec_time:.2f} ms")
            return result, exec_time
        except Exception as e:
            print(f"BillingService: Error al generar factura: {str(e)}")
            raise # Re-lanzar para que la UI lo maneje
