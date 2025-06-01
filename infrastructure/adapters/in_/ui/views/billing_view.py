import streamlit as st
import json
import pandas as pd
from application.services.billing_service import BillingService
from application.services.entity_service import EntityService

@st.cache_data(ttl=3600)
def get_cached_entity_data(_entity_service: EntityService, table_name: str):
    return _entity_service.get_entity_data(table_name)

def billing_tab_view(billing_service: BillingService, entity_service: EntityService, available_db_types: list, current_db_type: str):
    st.header("Proceso de Facturación (Simulador)")

    if not current_db_type:
        st.warning("Por favor, conecte a una base de datos desde la barra lateral para usar esta función.")
        return

    clientes_df = get_cached_entity_data(entity_service, 'Clientes')
    vendedores_df = get_cached_entity_data(entity_service, 'Personal')
    productos_df = get_cached_entity_data(entity_service, 'Producto')

    clientes_options = ["Seleccione un cliente"] + [f"{row['cliente_id']} - {row['nombre']}" for index, row in clientes_df.iterrows()]
    vendedores_options = ["Seleccione un vendedor"] + [f"{row['personal_id']} - {row['nombre']}" for index, row in vendedores_df.iterrows()]

    if 'current_invoice_products' not in st.session_state:
        st.session_state.current_invoice_products = []

    col1_main, col2_main = st.columns(2)
    with col1_main:
        selected_cliente = st.selectbox("Cliente", options=clientes_options, index=0)
    with col2_main:
        selected_vendedor = st.selectbox("Vendedor", options=vendedores_options, index=0)
    
    st.markdown("---")

    st.subheader("Agregar Productos a la Factura")
    if not productos_df.empty:
        productos_options = ["Seleccione un producto"] + [f"{row['producto_id']} - {row['nombre']} (S/. {row['precio']:.2f})" for index, row in productos_df.iterrows()]
        
        col_prod_select, col_prod_qty, col_prod_btn = st.columns([0.6, 0.2, 0.2])
        with col_prod_select:
            selected_product_option = st.selectbox("Producto", options=productos_options, index=0, key="product_selector_add")
        with col_prod_qty:
            product_quantity = st.number_input("Cantidad", min_value=1, value=1, step=1, key="product_quantity_add")
        with col_prod_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Agregar Producto", key="add_product_button"):
                if selected_product_option != "Seleccione un producto":
                    product_id = int(selected_product_option.split(' - ')[0])
                    selected_product_data = productos_df[productos_df['producto_id'] == product_id].iloc[0]
                    product_name = selected_product_data['nombre']
                    product_price = float(selected_product_data['precio'])

                    st.session_state.current_invoice_products.append({
                        "producto_id": product_id,
                        "nombre": product_name,
                        "cantidad": product_quantity,
                        "precio_unitario": product_price
                    })
                    st.success(f"'{product_name}' (x{product_quantity}) agregado a la factura.")
                else:
                    st.warning("Por favor, seleccione un producto para agregar.")
    else:
        st.info("No hay productos disponibles en la base de datos para agregar.")

    st.subheader("Productos en la Factura")
    if st.session_state.current_invoice_products:
        invoice_products_df = pd.DataFrame(st.session_state.current_invoice_products)
        st.dataframe(invoice_products_df[['nombre', 'cantidad', 'precio_unitario']], use_container_width=True, hide_index=True)
        
        if st.button("Limpiar Productos de la Factura", key="clear_products_button"):
            st.session_state.current_invoice_products = []
            st.info("Lista de productos de la factura limpiada.")
            st.rerun()
    else:
        st.info("No hay productos agregados a la factura aún.")

    st.markdown("---")

    st.caption(f"La factura se generará en la base de datos conectada: **{current_db_type}**")
    if st.button("Generar Factura", key="generate_invoice_final_button"):
        cliente_id = None
        if selected_cliente != "Seleccione un cliente":
            cliente_id = int(selected_cliente.split(' - ')[0])
        
        personal_id = None
        if selected_vendedor != "Seleccione un vendedor":
            personal_id = int(selected_vendedor.split(' - ')[0])

        if not cliente_id or not personal_id or not st.session_state.current_invoice_products:
            st.error("Por favor, seleccione un Cliente, un Vendedor y agregue al menos un Producto.")
            return

        products_for_json = [{"producto_id": item['producto_id'], "cantidad": item['cantidad']} for item in st.session_state.current_invoice_products]
        productos_json_str = json.dumps(products_for_json)
        
        try:
            with st.spinner(f"Generando factura en {current_db_type}..."):
                _result, exec_time = billing_service.generate_invoice_process(
                    client_id=cliente_id,
                    staff_id=personal_id,
                    products_json_str=productos_json_str
                )
            st.success(
                f"Factura generada exitosamente en {current_db_type} en {exec_time:.2f} ms."
            )
            if _result:
                st.info(f"Resultado del SP (ID Factura, Total): {_result}")
            else:
                st.warning("El SP no devolvió un resultado.")
            st.session_state.current_invoice_products = []
            st.rerun()

        except Exception as e:
            st.error(f"Error al generar factura en {current_db_type}: {str(e)}")
