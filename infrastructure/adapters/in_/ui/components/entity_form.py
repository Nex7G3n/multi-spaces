import streamlit as st
from typing import Dict, Any, Optional

def display_entity_form(
    table_name: str, 
    fields_config: Dict[str, str], 
    pk_col: str
) -> Optional[Dict[str, Any]]:
    """
    Muestra un formulario de Streamlit para agregar un nuevo registro a una entidad.

    Args:
        table_name (str): Nombre de la tabla/entidad.
        fields_config (Dict[str, str]): Configuración de los campos y sus tipos.
                                         Ej: {"nombre": "str", "precio": "decimal"}
        pk_col (str): Nombre de la columna de clave primaria (para excluirla del formulario).

    Returns:
        Optional[Dict[str, Any]]: Un diccionario con los datos del nuevo registro si el formulario
                                   fue enviado y es válido, None en caso contrario.
    """
    with st.form(key=f"add_form_{table_name}", clear_on_submit=True):
        st.subheader(f"Agregar Nuevo Registro a {table_name.replace('_', ' ')}")
        
        new_entry_data = {}
        form_columns = st.columns(2)  # Organizar en 2 columnas

        # Filtrar la clave primaria para no incluirla en el formulario de adición
        field_keys = [field for field in fields_config.keys() if field != pk_col]
        
        for i, field_name in enumerate(field_keys):
            field_type = fields_config[field_name]
            target_column = form_columns[i % 2]  # Alternar columnas

            with target_column:
                label = field_name.replace('_', ' ').capitalize()
                input_key = f"add_{table_name}_{field_name}"

                if field_type == "str":
                    new_entry_data[field_name] = st.text_input(label, key=input_key)
                elif field_type == "int":
                    new_entry_data[field_name] = st.number_input(label, step=1, value=None, key=input_key)
                elif field_type == "decimal":
                    # Usar format="%.2f" para asegurar dos decimales, value=None para permitir campo vacío
                    new_entry_data[field_name] = st.number_input(label, format="%.2f", value=None, key=input_key)
                elif field_type == "datetime":
                    # st.date_input devuelve un objeto date. Si se necesita datetime, usar st.datetime_input
                    new_entry_data[field_name] = st.date_input(label, value=None, key=input_key)
                else:
                    new_entry_data[field_name] = st.text_input(f"{label} (tipo desconocido)", key=input_key)

        submitted = st.form_submit_button("Agregar Registro")

        if submitted:
            # Validación simple: verificar que al menos un campo (además de fechas) tenga valor.
            # Las fechas siempre tendrán un valor por defecto si no se pone None.
            # Aquí se podría añadir una validación más compleja si es necesario.
            
            # Convertir campos numéricos vacíos (None) a un valor que la BD pueda manejar o rechazar.
            # Por ahora, se envían como None si el usuario no ingresa nada.
            # La base de datos decidirá si acepta NULLs según la definición de la tabla.

            # Filtrar campos que son None si no son obligatorios o si la BD los maneja.
            # Por simplicidad, se envían todos los campos.
            # Si un campo es None y la columna en la BD no permite NULLs, la inserción fallará.
            
            # Una validación básica podría ser:
            # if not any(v for k, v in new_entry_data.items() if fields_config[k] != "datetime" and v is not None and str(v).strip() != ""):
            #     st.warning("Por favor, complete al menos un campo.")
            #     return None # No enviar datos si está completamente vacío (excepto fechas)

            return new_entry_data
            
    return None
