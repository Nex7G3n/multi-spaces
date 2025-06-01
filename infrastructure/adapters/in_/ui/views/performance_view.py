import streamlit as st
from application.services.performance_service import PerformanceService
from shared.performance_data import add_performance_metric

def performance_test_view(performance_service: PerformanceService, db_type_selected: str):
    st.header("Pruebas de Rendimiento")

    test_operations_config = [
        ("Búsqueda de cliente", "search_client"),
        ("Búsqueda de producto", "search_product"),
        ("Generación de factura", "generate_invoice"),
        ("Consulta de factura", "query_invoice"),
        ("Reporte de ventas", "sales_report")
    ]

    if st.button("Ejecutar Todas las Pruebas de Rendimiento"):
        if not db_type_selected:
            st.error("Por favor, conecte a una base de datos primero desde la barra lateral.")
            return

        progress_bar = st.progress(0)
        status_text = st.empty()
        total_ops = len(test_operations_config)

        performance_service.clear_all_performance_data()
        st.info("Datos de rendimiento anteriores limpiados. Ejecutando nuevas pruebas...")

        for i, (op_name_display, op_method_name) in enumerate(test_operations_config):
            status_text.text(f"Ejecutando: {op_name_display} en {db_type_selected}...")
            try:
                if hasattr(performance_service.repository, op_method_name) and \
                   callable(getattr(performance_service.repository, op_method_name)):
                    
                    method_to_call_on_repo = getattr(performance_service.repository, op_method_name)
                    
                    _result, exec_time = method_to_call_on_repo() 
                    
                    add_performance_metric(db_type_selected, op_name_display, exec_time)
                    
                    st.write(f"{db_type_selected} - {op_name_display}: OK ({exec_time:.2f} ms)")
                else:
                    st.write(f"{db_type_selected} - {op_name_display}: Error - Método '{op_method_name}' no encontrado en el repositorio.")
                    add_performance_metric(db_type_selected, op_name_display, -1.0)


            except Exception as e:
                st.write(f"{db_type_selected} - {op_name_display}: Error - {str(e)}")
                add_performance_metric(db_type_selected, op_name_display, -1.0)
            
            progress_bar.progress((i + 1) / total_ops)
        
        status_text.text("Pruebas de rendimiento completadas!")
        st.balloons()
