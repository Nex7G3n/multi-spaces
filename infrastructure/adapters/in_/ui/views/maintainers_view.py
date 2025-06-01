import streamlit as st
import pandas as pd
from datetime import date, datetime

from application.services.entity_service import EntityService
from infrastructure.adapters.in_.ui.components.entity_form import display_entity_form
from infrastructure.adapters.out.persistence.config.table_definitions import TABLE_DEFINITIONS

def maintainers_tab_view(entity_service: EntityService):
    st.header("Mantenedores de Entidades")

    excluded_tables = ["Factura", "Detalle_Factura"]
    table_names = [name for name in TABLE_DEFINITIONS.keys() if name not in excluded_tables]
    selected_table_name = st.selectbox(
        "Seleccione la tabla a mantener",
        table_names,
        key="mantenedor_table_select_view"
    )

    if not selected_table_name:
        st.info("Por favor seleccione una tabla.")
        return

    table_config = TABLE_DEFINITIONS[selected_table_name]
    pk_col = table_config["pk"]
    fields_config = table_config["fields"]

    with st.expander(f"Agregar Nuevo Registro a {selected_table_name.replace('_', ' ')}"):
        new_data = display_entity_form(selected_table_name, fields_config, pk_col)
        if new_data:
            try:
                is_valid = True
                for field, value in new_data.items():
                    if fields_config[field] in ["str"] and (value is None or not str(value).strip()):
                        pass
                    elif fields_config[field] in ["int", "decimal"] and value is None:
                        pass

                if is_valid:
                    entity_service.add_entity(selected_table_name, new_data)
                    st.success(f"Registro agregado exitosamente a {selected_table_name}!")
            except Exception as e:
                st.error(f"Error al agregar registro a {selected_table_name}: {str(e)}")

    st.subheader(f"Listado de {selected_table_name.replace('_', ' ')}")
    
    current_data_df = entity_service.get_entity_data(selected_table_name)

    if current_data_df.empty and pk_col not in current_data_df.columns :
        st.info(f"No hay datos en la tabla {selected_table_name} o la tabla no se pudo cargar.")
        column_names = list(fields_config.keys())
        current_data_df = pd.DataFrame(columns=column_names)

    if selected_table_name == "Clientes" and not current_data_df.empty and pk_col in current_data_df.columns:
        try:
            current_data_df[pk_col] = pd.to_numeric(current_data_df[pk_col])
        except Exception:
            pass
        current_data_df = current_data_df.sort_values(by=pk_col, ascending=True).reset_index(drop=True)
    
    st.info("Para eliminar un registro, selecciónelo y use el icono de la papelera en la tabla.")

    column_editor_config = {pk_col: st.column_config.Column(disabled=True)}
    for col, type_hint in fields_config.items():
        if type_hint == "datetime":
            column_editor_config[col] = st.column_config.DateColumn(
                format="YYYY-MM-DD",
            )
        elif type_hint == "decimal":
             column_editor_config[col] = st.column_config.NumberColumn(
                format="%.2f"
            )

    edited_data = st.data_editor(
        current_data_df,
        num_rows="dynamic",
        use_container_width=True,
        key=f'{selected_table_name}_data_editor',
        column_config=column_editor_config,
    )
    
    changes_processed_flag = False

    current_data_indexed = current_data_df.set_index(pk_col, drop=False)
    edited_data_indexed = edited_data.set_index(pk_col, drop=False)

    original_ids = set(current_data_indexed.index)
    edited_ids = set(edited_data_indexed.index)

    deleted_ids = list(original_ids - edited_ids)
    
    if deleted_ids:
        for record_id_to_delete in deleted_ids:
            try:
                entity_service.delete_entity(selected_table_name, record_id_to_delete)
                st.success(f"Registro (ID: {record_id_to_delete}) eliminado de {selected_table_name}.")
                changes_processed_flag = True
            except Exception as e:
                st.error(f"Error eliminando ID {record_id_to_delete}: {e}")
    
    common_ids = list(original_ids.intersection(edited_ids))
    
    if common_ids:
        current_common = current_data_indexed.loc[common_ids]
        edited_common = edited_data_indexed.loc[common_ids]

        for record_id_to_update in common_ids:
            original_row = current_common.loc[record_id_to_update]
            edited_row = edited_common.loc[record_id_to_update]
            
            update_payload = {}
            for col_name, original_value in original_row.items():
                if col_name == pk_col:
                    continue
                new_value = edited_row[col_name]
                
                if not pd.Series([original_value]).equals(pd.Series([new_value])):
                    field_type = fields_config.get(col_name)
                    if field_type == "datetime" and isinstance(new_value, str):
                        try:
                            update_payload[col_name] = pd.to_datetime(new_value).date()
                        except ValueError:
                            st.warning(f"Valor de fecha inválido '{new_value}' para '{col_name}'. Se omite.")
                            continue
                    elif field_type == "decimal" and new_value is not None:
                         update_payload[col_name] = float(new_value)
                    elif field_type == "int" and new_value is not None:
                         update_payload[col_name] = int(new_value)
                    else:
                        update_payload[col_name] = new_value
            
            if update_payload:
                try:
                    entity_service.update_entity(selected_table_name, record_id_to_update, update_payload)
                    st.success(f"Registro (ID: {record_id_to_update}) actualizado en {selected_table_name}.")
                    changes_processed_flag = True
                except Exception as e:
                    st.error(f"Error actualizando ID {record_id_to_update}: {e}")

    added_ids = list(edited_ids - original_ids)
    
    if added_ids:
        for record_id_added in added_ids:
            new_row_data_dict = edited_data_indexed.loc[record_id_added].to_dict()
            
            if pk_col in new_row_data_dict and (new_row_data_dict[pk_col] is None or str(new_row_data_dict[pk_col]).strip() == ""):
                del new_row_data_dict[pk_col]
            
            insert_payload = {}
            is_empty_row = True
            for col_name, value in new_row_data_dict.items():
                if value is not None and str(value).strip() != "":
                    is_empty_row = False
                field_type = fields_config.get(col_name)
                if field_type == "datetime" and isinstance(value, str):
                    try:
                        insert_payload[col_name] = pd.to_datetime(value).date()
                    except ValueError:
                        insert_payload[col_name] = None 
                elif field_type == "decimal" and value is not None:
                    insert_payload[col_name] = float(value)
                elif field_type == "int" and value is not None:
                    insert_payload[col_name] = int(value)
                else:
                    insert_payload[col_name] = value
            
            if not insert_payload or is_empty_row: 
                continue

            try:
                entity_service.add_entity(selected_table_name, insert_payload)
                st.success(f"Nuevo registro agregado a {selected_table_name} desde la tabla.")
                changes_processed_flag = True
            except Exception as e:
                st.error(f"Error agregando nuevo registro desde la tabla: {e}")
    
    if changes_processed_flag:
        st.rerun()
